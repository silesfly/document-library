"""Frozen metadata publication v1 SQL and DB-only derivative/restore helpers.

0017 imports STATEMENTS_V1. Future schema versions must keep these semantics.
No helper here opens a connection, writer, file, service or network route.
"""
import json
import uuid

from .library_contracts import canonical_json

COVER_SCHEMA = 'metadata.cover.v1'
ORIGINS = ('EDITORIAL', 'PUBLISHER_OFFICIAL', 'QWEN_TEXT', 'QWEN_OCR', 'EMBEDDED', 'FIRST_PAGE')
LOCAL_ORIGINS = frozenset({'QWEN_TEXT', 'QWEN_OCR', 'EMBEDDED', 'FIRST_PAGE'})
PRIORITY = dict(EDITORIAL=0, PUBLISHER_OFFICIAL=1, QWEN_TEXT=2, QWEN_OCR=3, EMBEDDED=2, FIRST_PAGE=3)
COVER_REFERENCES_V1 = "SELECT c.object_sha,c.id AS candidate_id FROM metadata_candidates c JOIN metadata_candidate_control k ON k.candidate_id=c.id WHERE c.kind='cover' AND k.retained=1"
HISTORICAL_COVERS_V1 = "SELECT c.object_sha FROM metadata_candidates c WHERE c.kind='cover' AND NOT EXISTS(SELECT 1 FROM metadata_retention_tombstones t WHERE t.candidate_id=c.id)"


def _sha(column):
    return f"length({column})=64 AND {column} NOT GLOB '*[^0-9a-f]*'"


def _object(column, maximum):
    return f"json_valid({column}) AND json_type({column})='object' AND length(CAST({column} AS BLOB))<={maximum}"


def _id(column):
    return f'length({column}) BETWEEN 1 AND 200'


def _table(name, columns):
    return f'CREATE TABLE {name} ({columns}) STRICT'


def _trigger(name, event, table, body, when=None):
    return f'CREATE TRIGGER {name} {event} ON {table} '+(f'WHEN {when} ' if when else '')+f'BEGIN {body} END'


def _dirty(work):
    return f"""UPDATE storage_sequence SET value=value+1 WHERE id=1;
    INSERT INTO metadata_backup_dirty(work_id,sequence) SELECT {work},value FROM storage_sequence WHERE id=1
    ON CONFLICT(work_id) DO UPDATE SET sequence=max(sequence,excluded.sequence);"""


def _refresh(work):
    return f"""UPDATE library_metadata SET
      description_text=(SELECT description_text FROM metadata_expected_heads_v1 WHERE work_id={work}),
      description_state=(SELECT description_state FROM metadata_expected_heads_v1 WHERE work_id={work}),
      description_origin=(SELECT description_origin FROM metadata_expected_heads_v1 WHERE work_id={work}),
      summary_state=(SELECT summary_state FROM metadata_expected_heads_v1 WHERE work_id={work})
      WHERE work_id={work};"""


def _invalidate(work):
    return f'UPDATE metadata_heads SET version=version+1 WHERE work_id={work};'


def _image(alias):
    return "json_object("+','.join(f"'{key}',{alias}.{key}" for key in (
        'candidate_id','version','review_status','retained','minimum_rights_version','actor_kind','actor_id',
        'request_id','updated_at_ms','source_snapshot_sha256','audit_context_json'))+")"


_actor_check = "CHECK((actor_kind='admin' AND actor_id IS NOT NULL AND source_snapshot_sha256 IS NULL) OR (actor_kind='system_retention' AND actor_id IS NULL AND source_snapshot_sha256 IS NULL) OR (actor_kind='system_restore' AND actor_id IS NULL AND source_snapshot_sha256 IS NOT NULL))"
_tables = [
    _table('metadata_rights', f"""id TEXT PRIMARY KEY NOT NULL CHECK({_id('id')}), work_id TEXT NOT NULL REFERENCES works(id),
      kind TEXT NOT NULL CHECK(kind IN ('description','cover')), version INTEGER NOT NULL CHECK(version>=1),
      state TEXT NOT NULL DEFAULT 'review' CHECK(state IN ('allowed','review','blocked')),
      match_state TEXT NOT NULL DEFAULT 'review' CHECK(match_state IN ('exact','review','not_applicable')),
      officiality TEXT NOT NULL DEFAULT 'unknown' CHECK(officiality IN ('verified','unknown','not_applicable')),
      display_allowed INTEGER NOT NULL DEFAULT 0 CHECK(display_allowed IN (0,1)),
      cache_allowed INTEGER NOT NULL DEFAULT 0 CHECK(cache_allowed IN (0,1)),
      derivative_allowed INTEGER NOT NULL DEFAULT 0 CHECK(derivative_allowed IN (0,1)),
      evidence_json TEXT NOT NULL CHECK({_object('evidence_json',16384)}),
      retention_policy_json TEXT NOT NULL CHECK({_object('retention_policy_json',4096)}),
      cache_until_ms INTEGER CHECK(cache_until_ms>=0), decision_actor TEXT NOT NULL REFERENCES users(id),
      request_id TEXT UNIQUE NOT NULL CHECK({_id('request_id')}), updated_at_ms INTEGER NOT NULL CHECK(updated_at_ms>=0),
      UNIQUE(id,work_id,kind)"""),
    _table('metadata_rights_history', f"""rights_id TEXT NOT NULL REFERENCES metadata_rights(id), version INTEGER NOT NULL CHECK(version>=1),
      decision_json TEXT NOT NULL CHECK({_object('decision_json',16384)}), request_id TEXT UNIQUE NOT NULL,
      actor_id TEXT NOT NULL REFERENCES users(id), recorded_at_ms INTEGER NOT NULL CHECK(recorded_at_ms>=0), PRIMARY KEY(rights_id,version)"""),
    _table('metadata_job_inputs', f"""job_id TEXT PRIMARY KEY NOT NULL REFERENCES jobs(id), work_id TEXT NOT NULL REFERENCES works(id),
      rights_id TEXT NOT NULL REFERENCES metadata_rights(id), expected_rights_version INTEGER NOT NULL CHECK(expected_rights_version>=1),
      expected_head_version INTEGER NOT NULL CHECK(expected_head_version>=0), origin TEXT NOT NULL CHECK(origin IN {ORIGINS}),
      source_revision_id TEXT REFERENCES edition_revisions(id), source_binding_json TEXT NOT NULL CHECK({_object('source_binding_json',65536)}),
      request_digest TEXT NOT NULL CHECK({_sha('request_digest')}), actor_id TEXT NOT NULL REFERENCES users(id),
      actor_authz_version INTEGER NOT NULL CHECK(actor_authz_version>=1), created_at_ms INTEGER NOT NULL CHECK(created_at_ms>=0)"""),
    _table('metadata_candidates', f"""id TEXT PRIMARY KEY NOT NULL CHECK({_id('id')}), work_id TEXT NOT NULL REFERENCES works(id),
      kind TEXT NOT NULL CHECK(kind IN ('description','cover')), origin TEXT NOT NULL CHECK(origin IN {ORIGINS}),
      rights_id TEXT NOT NULL, rights_version_at_creation INTEGER NOT NULL CHECK(rights_version_at_creation>=1),
      source_revision_id TEXT REFERENCES edition_revisions(id), object_sha TEXT REFERENCES objects(sha256),
      source_url TEXT CHECK(length(source_url)<=2048), content_sha256 TEXT NOT NULL CHECK({_sha('content_sha256')}),
      provenance_json TEXT NOT NULL CHECK({_object('provenance_json',65536)}),
      producing_job_id TEXT REFERENCES jobs(id), generation INTEGER CHECK(generation>=1),
      created_at_ms INTEGER NOT NULL CHECK(created_at_ms>=0), UNIQUE(id,work_id),
      FOREIGN KEY(rights_id,work_id,kind) REFERENCES metadata_rights(id,work_id,kind),
      CHECK((kind='description' AND object_sha IS NULL) OR (kind='cover' AND object_sha IS NOT NULL AND object_sha=content_sha256)),
      CHECK((producing_job_id IS NULL AND generation IS NULL AND origin='EDITORIAL') OR (producing_job_id IS NOT NULL AND generation IS NOT NULL)),
      CHECK(origin NOT IN ('QWEN_TEXT','QWEN_OCR','EMBEDDED','FIRST_PAGE') OR source_revision_id IS NOT NULL)"""),
    _table('metadata_candidate_sources', f"""candidate_id TEXT NOT NULL REFERENCES metadata_candidates(id),
      source_revision_id TEXT NOT NULL REFERENCES edition_revisions(id), proof_role TEXT NOT NULL CHECK(proof_role IN ('root','asset')),
      asset_position INTEGER NOT NULL CHECK((proof_role='root' AND asset_position=-1) OR (proof_role='asset' AND asset_position>=0)),
      source_job_id TEXT NOT NULL, source_generation INTEGER NOT NULL CHECK(source_generation>=1),
      expected_payload_digest TEXT NOT NULL CHECK({_sha('expected_payload_digest')}),
      PRIMARY KEY(candidate_id,proof_role,asset_position,source_job_id,source_generation),
      FOREIGN KEY(source_job_id,source_generation) REFERENCES publication_plans(job_id,generation)"""),
    _table('metadata_candidate_control', f"""candidate_id TEXT PRIMARY KEY NOT NULL REFERENCES metadata_candidates(id),
      version INTEGER NOT NULL CHECK(version>=1), review_status TEXT NOT NULL CHECK(review_status IN ('pending','approved','rejected')),
      retained INTEGER NOT NULL CHECK(retained IN (0,1)), minimum_rights_version INTEGER NOT NULL CHECK(minimum_rights_version>=1),
      actor_kind TEXT NOT NULL CHECK(actor_kind IN ('admin','system_restore','system_retention')), actor_id TEXT REFERENCES users(id),
      request_id TEXT UNIQUE NOT NULL CHECK({_id('request_id')}), updated_at_ms INTEGER NOT NULL CHECK(updated_at_ms>=0),
      source_snapshot_sha256 TEXT CHECK({_sha('source_snapshot_sha256')}), audit_context_json TEXT NOT NULL DEFAULT '{{}}' CHECK({_object('audit_context_json',4096)}),
      {_actor_check}"""),
    _table('metadata_control_events', f"""id TEXT PRIMARY KEY NOT NULL, candidate_id TEXT NOT NULL REFERENCES metadata_candidates(id),
      request_id TEXT NOT NULL, action TEXT NOT NULL, before_version INTEGER NOT NULL CHECK(before_version>=0),
      after_version INTEGER NOT NULL CHECK(after_version=before_version+1), before_json TEXT NOT NULL CHECK({_object('before_json',8192)}),
      after_json TEXT NOT NULL CHECK({_object('after_json',8192)}), actor_kind TEXT NOT NULL CHECK(actor_kind IN ('admin','system_restore','system_retention')),
      actor_id TEXT REFERENCES users(id), source_snapshot_sha256 TEXT CHECK({_sha('source_snapshot_sha256')}),
      recorded_at_ms INTEGER NOT NULL CHECK(recorded_at_ms>=0), UNIQUE(candidate_id,request_id,action), {_actor_check}"""),
    _table('metadata_heads', """work_id TEXT PRIMARY KEY NOT NULL REFERENCES works(id) ON DELETE CASCADE, description_id TEXT, cover_id TEXT,
      version INTEGER NOT NULL DEFAULT 0 CHECK(version>=0), updated_at_ms INTEGER NOT NULL DEFAULT 0 CHECK(updated_at_ms>=0),
      FOREIGN KEY(description_id,work_id) REFERENCES metadata_candidates(id,work_id),
      FOREIGN KEY(cover_id,work_id) REFERENCES metadata_candidates(id,work_id)"""),
    _table('metadata_backup_dirty', 'work_id TEXT PRIMARY KEY NOT NULL REFERENCES works(id) ON DELETE CASCADE, sequence INTEGER NOT NULL CHECK(sequence>=0)'),
    _table('metadata_payloads', f"""candidate_id TEXT PRIMARY KEY NOT NULL REFERENCES metadata_candidates(id),
      text_value TEXT NOT NULL CHECK(length(text_value) BETWEEN 1 AND 16384), text_sha256 TEXT NOT NULL CHECK({_sha('text_sha256')}),
      CHECK(library_plan_valid_v1(text_value,text_sha256)=1)"""),
    _table('metadata_retention_tombstones', f"""candidate_id TEXT PRIMARY KEY NOT NULL REFERENCES metadata_candidates(id),
      rights_version INTEGER NOT NULL CHECK(rights_version>=1), reason TEXT NOT NULL CHECK(reason IN ('cache_revoked','cache_expired','required_deletion')),
      requested_at_ms INTEGER NOT NULL CHECK(requested_at_ms>=0), requested_sequence INTEGER NOT NULL CHECK(requested_sequence>=0),
      backup_obligation_json TEXT NOT NULL CHECK({_object('backup_obligation_json',4096)})"""),
]

_views = [
    """CREATE VIEW metadata_actual_sources_v1 AS
      SELECT s.revision_id, 'root' AS proof_role,-1 AS asset_position,s.source_job_id,s.source_generation,p.payload_digest
      FROM revision_seals s JOIN publication_plans p ON p.job_id=s.source_job_id AND p.generation=s.source_generation
      UNION ALL SELECT s.edition_revision_id,'asset',s.asset_position,s.source_job_id,s.source_generation,p.payload_digest
      FROM revision_asset_sources s JOIN publication_plans p ON p.job_id=s.source_job_id AND p.generation=s.source_generation""",
    """CREATE VIEW metadata_eligible_candidates_v1 AS SELECT c.* FROM metadata_candidates c
      JOIN metadata_rights r ON r.id=c.rights_id JOIN metadata_candidate_control k ON k.candidate_id=c.id
      WHERE k.review_status='approved' AND r.version>=k.minimum_rights_version AND r.state='allowed'
      AND r.display_allowed=1 AND r.cache_allowed=1
      AND NOT EXISTS(SELECT 1 FROM metadata_retention_tombstones t WHERE t.candidate_id=c.id)
      AND (c.origin<>'PUBLISHER_OFFICIAL' OR (r.officiality='verified' AND r.match_state='exact'))
      AND (c.kind<>'description' OR EXISTS(SELECT 1 FROM metadata_payloads b WHERE b.candidate_id=c.id AND b.text_sha256=c.content_sha256 AND library_plan_valid_v1(b.text_value,c.content_sha256)=1))
      AND (c.kind<>'cover' OR (k.retained=1 AND EXISTS(SELECT 1 FROM objects o WHERE o.sha256=c.object_sha AND o.state='LIVE' AND o.local_relpath='objects/'||substr(o.sha256,1,2)||'/'||substr(o.sha256,3,2)||'/'||o.sha256)))
      AND (c.origin NOT IN ('QWEN_TEXT','QWEN_OCR','EMBEDDED','FIRST_PAGE') OR (r.derivative_allowed=1
        AND EXISTS(SELECT 1 FROM edition_revisions v JOIN work_editions e ON e.id=v.edition_id WHERE v.id=c.source_revision_id AND e.work_id=c.work_id)
        AND EXISTS(SELECT 1 FROM revision_assets a WHERE a.edition_revision_id=c.source_revision_id)
        AND EXISTS(SELECT 1 FROM metadata_candidate_sources s WHERE s.candidate_id=c.id AND s.proof_role='root')
        AND NOT EXISTS(SELECT 1 FROM revision_assets a WHERE a.edition_revision_id=c.source_revision_id AND NOT EXISTS(SELECT 1 FROM metadata_candidate_sources s WHERE s.candidate_id=c.id AND s.proof_role='asset' AND s.asset_position=a.asset_position))
        AND NOT EXISTS(SELECT proof_role,asset_position,source_job_id,source_generation,payload_digest FROM metadata_actual_sources_v1 WHERE revision_id=c.source_revision_id
          EXCEPT SELECT proof_role,asset_position,source_job_id,source_generation,expected_payload_digest FROM metadata_candidate_sources WHERE candidate_id=c.id)
        AND NOT EXISTS(SELECT proof_role,asset_position,source_job_id,source_generation,expected_payload_digest FROM metadata_candidate_sources WHERE candidate_id=c.id
          EXCEPT SELECT proof_role,asset_position,source_job_id,source_generation,payload_digest FROM metadata_actual_sources_v1 WHERE revision_id=c.source_revision_id)
        AND NOT EXISTS(SELECT 1 FROM metadata_candidate_sources s JOIN publication_plans p ON p.job_id=s.source_job_id AND p.generation=s.source_generation
          LEFT JOIN ingestion_inputs i ON i.job_id=p.job_id WHERE s.candidate_id=c.id AND
          (p.rights_status<>'allowed' OR library_plan_valid_v1(p.payload_json,s.expected_payload_digest)<>1 OR json_extract(p.payload_json,'$.schema')='metadata.cover.v1' OR (i.job_id IS NOT NULL AND i.rights_status<>'allowed')))))
      AND (c.producing_job_id IS NULL OR c.kind<>'cover' OR EXISTS(SELECT 1 FROM publication_plans p WHERE p.job_id=c.producing_job_id AND p.generation=c.generation AND p.rights_status='allowed' AND library_plan_valid_v1(p.payload_json,p.payload_digest)=1 AND json_extract(p.payload_json,'$.schema')='metadata.cover.v1' AND json_extract(p.payload_json,'$.candidate_id')=c.id AND json_extract(p.payload_json,'$.work_id')=c.work_id AND json_extract(p.payload_json,'$.rights_id')=c.rights_id AND json_extract(p.payload_json,'$.rights_version')=c.rights_version_at_creation AND json_extract(p.payload_json,'$.origin')=c.origin AND json_extract(p.payload_json,'$.asset.sha256')=c.object_sha))""",
    """CREATE VIEW metadata_expected_heads_v1 AS SELECT h.work_id,h.description_id,h.cover_id,h.version,
      CASE WHEN d.id IS NOT NULL THEN b.text_value WHEN h.description_id IS NULL THEN m.description_text ELSE NULL END AS description_text,
      CASE WHEN d.id IS NOT NULL THEN 'ready' ELSE 'pending' END AS description_state,
      CASE WHEN d.id IS NOT NULL THEN d.origin WHEN h.description_id IS NULL THEN m.description_origin ELSE NULL END AS description_origin,
      CASE WHEN d.origin IN ('QWEN_TEXT','QWEN_OCR') THEN 'ready' ELSE 'pending' END AS summary_state,
      CASE WHEN c.id IS NOT NULL THEN 'ready' ELSE 'pending' END AS cover_state,
      CASE WHEN d.source_revision_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM edition_revisions v WHERE v.id=d.source_revision_id AND v.is_active=1) THEN 'stale' ELSE 'current' END AS description_freshness
      FROM metadata_heads h JOIN library_metadata m ON m.work_id=h.work_id
      LEFT JOIN metadata_eligible_candidates_v1 d ON d.id=h.description_id AND d.kind='description'
      LEFT JOIN metadata_payloads b ON b.candidate_id=d.id
      LEFT JOIN metadata_eligible_candidates_v1 c ON c.id=h.cover_id AND c.kind='cover'""",
]

_guards = [_trigger('metadata_jobs_replace_v1','BEFORE INSERT','jobs',
    "SELECT RAISE(ABORT,'referenced metadata job cannot be replaced');",
    "EXISTS(SELECT 1 FROM jobs WHERE id=NEW.id) AND (EXISTS(SELECT 1 FROM metadata_job_inputs WHERE job_id=NEW.id) OR EXISTS(SELECT 1 FROM metadata_candidates WHERE producing_job_id=NEW.id) OR EXISTS(SELECT 1 FROM metadata_candidate_sources WHERE source_job_id=NEW.id))")]
for table in ('metadata_rights_history','metadata_candidates','metadata_candidate_sources','metadata_job_inputs','metadata_control_events','metadata_retention_tombstones'):
    for event in ('UPDATE','DELETE'):
        _guards.append(_trigger(f'{table}_{event.lower()}_immutable',f'BEFORE {event}',table,"SELECT RAISE(ABORT,'immutable metadata evidence');"))
    keys = {'metadata_rights_history':"rights_id=NEW.rights_id AND version=NEW.version",'metadata_candidate_sources':"candidate_id=NEW.candidate_id AND proof_role=NEW.proof_role AND asset_position=NEW.asset_position AND source_job_id=NEW.source_job_id AND source_generation=NEW.source_generation",'metadata_job_inputs':'job_id=NEW.job_id','metadata_retention_tombstones':'candidate_id=NEW.candidate_id'}.get(table,'id=NEW.id')
    _guards.append(_trigger(f'{table}_replace_immutable','BEFORE INSERT',table,"SELECT RAISE(ABORT,'metadata replacement forbidden');",f'EXISTS(SELECT 1 FROM {table} WHERE {keys})'))
for table,key,identity in [('metadata_rights','id','NEW.id<>OLD.id OR NEW.work_id<>OLD.work_id OR NEW.kind<>OLD.kind'),('metadata_candidate_control','candidate_id','NEW.candidate_id<>OLD.candidate_id'),('metadata_heads','work_id','NEW.work_id<>OLD.work_id')]:
    _guards.append(_trigger(table+'_version','BEFORE UPDATE',table,"SELECT RAISE(ABORT,'metadata version or identity conflict');",f'{identity} OR NEW.version<>OLD.version+1'+(' OR NEW.minimum_rights_version<OLD.minimum_rights_version' if table=='metadata_candidate_control' else '')))
    _guards.append(_trigger(table+'_replace','BEFORE INSERT',table,"SELECT RAISE(ABORT,'metadata replacement forbidden');",f'EXISTS(SELECT 1 FROM {table} WHERE {key}=NEW.{key})'))
    # Only the pristine derivative head may disappear with its parent. Real
    # rights/candidates/inputs and all versioned heads remain protected.
    delete_when=("EXISTS(SELECT 1 FROM works WHERE id=OLD.work_id) OR OLD.version<>0 OR OLD.description_id IS NOT NULL OR OLD.cover_id IS NOT NULL OR EXISTS(SELECT 1 FROM metadata_rights WHERE work_id=OLD.work_id) OR EXISTS(SELECT 1 FROM metadata_candidates WHERE work_id=OLD.work_id) OR EXISTS(SELECT 1 FROM metadata_job_inputs WHERE work_id=OLD.work_id)" if table=='metadata_heads' else None)
    _guards.append(_trigger(table+'_delete','BEFORE DELETE',table,"SELECT RAISE(ABORT,'metadata identity cannot be removed');",delete_when))
for event in ('INSERT','UPDATE'):
    for table in ('revision_seals','revision_asset_sources'):
        _guards.append(_trigger('metadata_not_'+table+'_'+event.lower(),'BEFORE '+event,table,"SELECT RAISE(ABORT,'metadata cover cannot approve original source');","EXISTS(SELECT 1 FROM publication_plans p WHERE p.job_id=NEW.source_job_id AND p.generation=NEW.source_generation AND json_extract(p.payload_json,'$.schema')='metadata.cover.v1')"))
    _guards.append(_trigger('metadata_head_guard_'+event.lower(),'BEFORE '+event,'metadata_heads',"SELECT RAISE(ABORT,'metadata head selection is not eligible');","(NEW.description_id IS NOT NULL AND "+("NEW.description_id IS NOT OLD.description_id AND " if event=='UPDATE' else '')+"NOT EXISTS(SELECT 1 FROM metadata_eligible_candidates_v1 c WHERE c.id=NEW.description_id AND c.work_id=NEW.work_id AND c.kind='description')) OR (NEW.cover_id IS NOT NULL AND (NOT EXISTS(SELECT 1 FROM metadata_candidate_control k WHERE k.candidate_id=NEW.cover_id AND k.retained=1) OR "+("NEW.cover_id IS NOT OLD.cover_id AND " if event=='UPDATE' else '')+"NOT EXISTS(SELECT 1 FROM metadata_eligible_candidates_v1 c WHERE c.id=NEW.cover_id AND c.work_id=NEW.work_id AND c.kind='cover')))"))
    _guards.append(_trigger('metadata_control_guard_'+event.lower(),'BEFORE '+event,'metadata_candidate_control',"SELECT RAISE(ABORT,'invalid metadata retention or tombstone');","(NEW.retained=1 AND NOT EXISTS(SELECT 1 FROM metadata_candidates c JOIN objects o ON o.sha256=c.object_sha WHERE c.id=NEW.candidate_id AND c.kind='cover' AND o.state='LIVE' AND o.local_relpath='objects/'||substr(o.sha256,1,2)||'/'||substr(o.sha256,3,2)||'/'||o.sha256) AND "+("NEW.retained<>OLD.retained" if event=='UPDATE' else '1')+") OR (NEW.retained=0 AND EXISTS(SELECT 1 FROM metadata_heads h WHERE h.cover_id=NEW.candidate_id)) OR ("+("NEW.retained=1 AND OLD.retained=0" if event=='UPDATE' else "NEW.retained=1")+" AND EXISTS(SELECT 1 FROM metadata_retention_tombstones t WHERE t.candidate_id=NEW.candidate_id))"))
    _guards.append(_trigger('metadata_projection_guard_'+event.lower(),'BEFORE '+event,'library_metadata',"SELECT RAISE(ABORT,'ready metadata requires eligible publication');","(NEW.description_state='ready' OR NEW.summary_state='ready') AND NOT EXISTS(SELECT 1 FROM metadata_expected_heads_v1 e WHERE e.work_id=NEW.work_id AND e.description_state='ready' AND e.description_text IS NEW.description_text AND e.description_origin IS NEW.description_origin AND e.summary_state=NEW.summary_state)"))
_guards += [
    _trigger('metadata_producer_guard','BEFORE INSERT','metadata_candidates',"SELECT RAISE(ABORT,'candidate producer metadata input differs');","NEW.producing_job_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM jobs j JOIN metadata_job_inputs i ON i.job_id=j.id WHERE j.id=NEW.producing_job_id AND j.state='running' AND j.lease_generation=NEW.generation AND NEW.generation>0 AND j.kind='metadata.'||NEW.kind||'.publish' AND j.work_id=NEW.work_id AND i.work_id=NEW.work_id AND j.input_revision_id IS i.source_revision_id AND NEW.source_revision_id IS i.source_revision_id AND i.origin=NEW.origin AND i.rights_id=NEW.rights_id AND i.expected_rights_version=NEW.rights_version_at_creation AND library_plan_valid_v1(j.payload_json,i.request_digest)=1 AND (NEW.kind='description' OR EXISTS(SELECT 1 FROM publication_plans p WHERE p.job_id=j.id AND p.generation=NEW.generation AND json_extract(p.payload_json,'$.schema')='metadata.cover.v1' AND json_extract(p.payload_json,'$.candidate_id')=NEW.id AND json_extract(p.payload_json,'$.asset.sha256')=NEW.object_sha AND json_extract(p.payload_json,'$.input_binding_digest')=i.request_digest)))"),
    _trigger('metadata_binding_guard','BEFORE INSERT','metadata_job_inputs',"SELECT RAISE(ABORT,'invalid or mixed metadata job binding');","EXISTS(SELECT 1 FROM ingestion_inputs WHERE job_id=NEW.job_id) OR NOT EXISTS(SELECT 1 FROM jobs j JOIN metadata_rights r ON r.id=NEW.rights_id WHERE j.id=NEW.job_id AND j.kind IN ('metadata.description.publish','metadata.cover.publish') AND j.work_id=NEW.work_id AND j.input_revision_id IS NEW.source_revision_id AND r.work_id=NEW.work_id AND r.kind=CASE j.kind WHEN 'metadata.cover.publish' THEN 'cover' ELSE 'description' END AND library_plan_valid_v1(j.payload_json,NEW.request_digest)=1 AND EXISTS(SELECT 1 FROM users u WHERE u.id=NEW.actor_id AND u.role='admin' AND u.status='active' AND u.authz_version=NEW.actor_authz_version))"),
    _trigger('ingestion_not_metadata','BEFORE INSERT','ingestion_inputs',"SELECT RAISE(ABORT,'metadata job cannot bind ingestion');","EXISTS(SELECT 1 FROM metadata_job_inputs WHERE job_id=NEW.job_id)"),
    _trigger('metadata_source_guard','BEFORE INSERT','metadata_candidate_sources',"SELECT RAISE(ABORT,'metadata source proof differs');","NOT EXISTS(SELECT 1 FROM metadata_candidates c JOIN metadata_actual_sources_v1 a ON a.revision_id=c.source_revision_id JOIN publication_plans p ON p.job_id=a.source_job_id AND p.generation=a.source_generation WHERE c.id=NEW.candidate_id AND c.source_revision_id=NEW.source_revision_id AND a.proof_role=NEW.proof_role AND a.asset_position=NEW.asset_position AND a.source_job_id=NEW.source_job_id AND a.source_generation=NEW.source_generation AND a.payload_digest=NEW.expected_payload_digest AND coalesce(json_extract(p.payload_json,'$.schema'),'')<>'metadata.cover.v1')"),
    _trigger('metadata_payload_guard','BEFORE INSERT','metadata_payloads',"SELECT RAISE(ABORT,'metadata payload lacks current cache permission');","EXISTS(SELECT 1 FROM metadata_retention_tombstones WHERE candidate_id=NEW.candidate_id) OR NOT EXISTS(SELECT 1 FROM metadata_candidates c JOIN metadata_rights r ON r.id=c.rights_id WHERE c.id=NEW.candidate_id AND c.kind='description' AND c.content_sha256=NEW.text_sha256 AND r.state='allowed' AND r.cache_allowed=1)"),
    _trigger('metadata_payload_update','BEFORE UPDATE','metadata_payloads',"SELECT RAISE(ABORT,'metadata payload is immutable until deletion');"),
]

_events = []
for event in ('INSERT','UPDATE'):
    decision="json_object("+','.join(f"'{key}',NEW.{key}" for key in ('state','officiality','match_state','display_allowed','cache_allowed','derivative_allowed','cache_until_ms','evidence_json','retention_policy_json'))+")"
    body=f"INSERT INTO metadata_rights_history VALUES(NEW.id,NEW.version,{decision},NEW.request_id,NEW.decision_actor,NEW.updated_at_ms);"
    body+=_dirty('NEW.work_id')
    body+="""INSERT INTO metadata_retention_tombstones
      SELECT c.id,NEW.version,CASE WHEN NEW.cache_until_ms IS NOT NULL AND NEW.cache_until_ms<=NEW.updated_at_ms THEN 'cache_expired' ELSE 'cache_revoked' END,
        NEW.updated_at_ms,(SELECT value FROM storage_sequence WHERE id=1),NEW.retention_policy_json
      FROM metadata_candidates c WHERE c.rights_id=NEW.id AND (NEW.state<>'allowed' OR NEW.cache_allowed=0 OR (NEW.cache_until_ms IS NOT NULL AND NEW.cache_until_ms<=NEW.updated_at_ms))
      AND NOT EXISTS(SELECT 1 FROM metadata_retention_tombstones t WHERE t.candidate_id=c.id);"""
    body+=_invalidate('NEW.work_id')
    _events.append(_trigger('metadata_rights_'+event.lower(),'AFTER '+event,'metadata_rights',body))
    before_image="'{}'" if event=='INSERT' else _image('OLD')
    before_version='0' if event=='INSERT' else 'OLD.version'
    body=f"INSERT INTO metadata_control_events VALUES(NEW.candidate_id||':'||NEW.version,NEW.candidate_id,NEW.request_id,NEW.actor_kind,{before_version},NEW.version,{before_image},{_image('NEW')},NEW.actor_kind,NEW.actor_id,NEW.source_snapshot_sha256,NEW.updated_at_ms);"
    body+="UPDATE object_lifecycle SET unreferenced_since=NULL WHERE object_sha IN (SELECT object_sha FROM metadata_candidates WHERE id=NEW.candidate_id);"
    work='(SELECT work_id FROM metadata_candidates WHERE id=NEW.candidate_id)'
    body+=_dirty(work)
    body+=f'UPDATE metadata_heads SET version=version+1 WHERE work_id={work} AND (description_id=NEW.candidate_id OR cover_id=NEW.candidate_id);'
    _events.append(_trigger('metadata_control_'+event.lower(),'AFTER '+event,'metadata_candidate_control',body))
_events += [
    _trigger('metadata_head_insert','AFTER INSERT','metadata_heads',_refresh('NEW.work_id')),
    _trigger('metadata_head_update','AFTER UPDATE','metadata_heads',_refresh('NEW.work_id')+_dirty('NEW.work_id')),
    _trigger('metadata_work_insert','AFTER INSERT','works','INSERT INTO metadata_heads(work_id) VALUES(NEW.id);'),
    _trigger('metadata_candidate_dirty','AFTER INSERT','metadata_candidates',_dirty('NEW.work_id')),
    _trigger('metadata_payload_delete','AFTER DELETE','metadata_payloads',"UPDATE library_metadata SET description_state='pending',description_text=NULL,description_origin=NULL,summary_state='pending' WHERE work_id=(SELECT work_id FROM metadata_candidates WHERE id=OLD.candidate_id) AND description_text=OLD.text_value;"+_invalidate('(SELECT work_id FROM metadata_candidates WHERE id=OLD.candidate_id)')),
    _trigger('metadata_tombstone_insert','AFTER INSERT','metadata_retention_tombstones',"DELETE FROM metadata_payloads WHERE candidate_id=NEW.candidate_id;"+_invalidate('(SELECT work_id FROM metadata_candidates WHERE id=NEW.candidate_id)')+_dirty('(SELECT work_id FROM metadata_candidates WHERE id=NEW.candidate_id)')),
]
for table, event, condition in [
    ('publication_plans','UPDATE',"c.id IN (SELECT candidate_id FROM metadata_candidate_sources WHERE (source_job_id=OLD.job_id AND source_generation=OLD.generation) OR (source_job_id=NEW.job_id AND source_generation=NEW.generation)) OR (c.producing_job_id=OLD.job_id AND c.generation=OLD.generation) OR (c.producing_job_id=NEW.job_id AND c.generation=NEW.generation)"),
    ('publication_plans','INSERT',"c.id IN (SELECT candidate_id FROM metadata_candidate_sources WHERE source_job_id=NEW.job_id AND source_generation=NEW.generation) OR (c.producing_job_id=NEW.job_id AND c.generation=NEW.generation)"),
    ('publication_plans','DELETE',"c.id IN (SELECT candidate_id FROM metadata_candidate_sources WHERE source_job_id=OLD.job_id AND source_generation=OLD.generation) OR (c.producing_job_id=OLD.job_id AND c.generation=OLD.generation)"),
    ('ingestion_inputs','INSERT',"c.id IN (SELECT candidate_id FROM metadata_candidate_sources WHERE source_job_id=NEW.job_id)"),
    ('ingestion_inputs','UPDATE',"c.id IN (SELECT candidate_id FROM metadata_candidate_sources WHERE source_job_id IN (OLD.job_id,NEW.job_id))"),
    ('ingestion_inputs','DELETE',"c.id IN (SELECT candidate_id FROM metadata_candidate_sources WHERE source_job_id=OLD.job_id)"),
    ('edition_revisions','INSERT',"c.source_revision_id=NEW.id"),
    ('edition_revisions','UPDATE',"c.source_revision_id IN (OLD.id,NEW.id)"),
    ('edition_revisions','DELETE',"c.source_revision_id=OLD.id"),
    ('work_editions','INSERT',"c.source_revision_id IN (SELECT id FROM edition_revisions WHERE edition_id=NEW.id)"),
    ('work_editions','UPDATE',"c.source_revision_id IN (SELECT id FROM edition_revisions WHERE edition_id IN (OLD.id,NEW.id))"),
    ('work_editions','DELETE',"c.source_revision_id IN (SELECT id FROM edition_revisions WHERE edition_id=OLD.id)"),
    ('objects','UPDATE OF state',"c.object_sha=NEW.sha256"),
]:
    work=f'(SELECT c.work_id FROM metadata_candidates c WHERE {condition})'
    _events.append(_trigger('metadata_invalidate_'+table+'_'+event.split()[0].lower(),'AFTER '+event,table,f'UPDATE metadata_heads SET version=version+1 WHERE work_id IN {work};',when='OLD.state IS NOT NEW.state' if table=='objects' else None))

STATEMENTS_V1 = tuple(_tables+_views+_guards+[
    'CREATE INDEX ix_metadata_source_reverse ON metadata_candidate_sources(source_job_id,source_generation,candidate_id)',
    'CREATE INDEX ix_metadata_candidate_work ON metadata_candidates(work_id,kind)',
    'CREATE INDEX ix_metadata_candidate_rights ON metadata_candidates(rights_id,id)',
    'CREATE INDEX ix_metadata_candidate_revision ON metadata_candidates(source_revision_id,id)',
    'CREATE INDEX ix_metadata_cover_object ON metadata_candidates(object_sha,id)',
    'INSERT INTO metadata_heads(work_id) SELECT id FROM works',
    "UPDATE library_metadata SET description_state='pending',summary_state='pending',cover_state='pending'",
]+_events)


def verify_metadata_publication_in(db):
    rows=db.execute("""SELECT m.work_id FROM library_metadata m JOIN metadata_expected_heads_v1 e ON e.work_id=m.work_id
      WHERE m.description_text IS NOT e.description_text OR m.description_state<>e.description_state
      OR m.description_origin IS NOT e.description_origin OR m.summary_state<>e.summary_state
      UNION ALL SELECT w.id FROM works w LEFT JOIN metadata_heads h ON h.work_id=w.id WHERE h.work_id IS NULL LIMIT 1""").fetchone()
    if rows:
        raise ValueError('Metadata publication projection differs')
    if db.execute("SELECT 1 FROM metadata_heads h LEFT JOIN metadata_candidate_control c ON c.candidate_id=h.cover_id WHERE h.cover_id IS NOT NULL AND coalesce(c.retained,0)<>1 LIMIT 1").fetchone():
        raise ValueError('Selected cover lacks retention')


def rebuild_metadata_publication_in(db):
    for (work,) in db.execute('SELECT work_id FROM metadata_heads ORDER BY work_id').fetchall():
        db.execute("""UPDATE library_metadata SET
          description_text=(SELECT description_text FROM metadata_expected_heads_v1 WHERE work_id=?),
          description_state=(SELECT description_state FROM metadata_expected_heads_v1 WHERE work_id=?),
          description_origin=(SELECT description_origin FROM metadata_expected_heads_v1 WHERE work_id=?),
          summary_state=(SELECT summary_state FROM metadata_expected_heads_v1 WHERE work_id=?) WHERE work_id=?""",(work,)*5)


def _verify_restore_replay_in(db, candidate, version, creation_rights_version, request, snapshot_sha256):
    """Prove an existing restore transition reaches the actual current control."""
    current=json.loads(db.execute('SELECT '+_image('k')+' FROM metadata_candidate_control k WHERE candidate_id=?',(candidate,)).fetchone()[0])
    previous={};next_version=1;found=False
    rows=db.execute("""SELECT id,before_version,after_version,before_json,after_json,actor_kind,actor_id,
      source_snapshot_sha256,recorded_at_ms,request_id,action FROM metadata_control_events
      WHERE candidate_id=? ORDER BY after_version,id""",(candidate,))
    try:
        for ident,before_version,after_version,before_json,after_json,actor,actor_id,snapshot,recorded_at,event_request,action in rows:
            before,after=json.loads(before_json),json.loads(after_json)
            if (ident!=candidate+':'+str(next_version) or before_version!=next_version-1 or after_version!=next_version
                    or after_version>version or before!=previous or set(after)!=set(current)
                    or after['candidate_id']!=candidate or after['version']!=after_version
                    or after['actor_kind']!=actor or after['actor_id']!=actor_id or after['source_snapshot_sha256']!=snapshot
                    or after['request_id']!=event_request or after['updated_at_ms']!=recorded_at or action!=actor
                    or type(after['minimum_rights_version']) is not int or after['minimum_rights_version']<1
                    or (previous and after['minimum_rights_version']<previous['minimum_rights_version'])):
                raise ValueError('Restore metadata replay evidence conflicts')
            if event_request==request and action=='system_restore':
                if (found or actor!='system_restore' or actor_id is not None or snapshot!=snapshot_sha256
                        or after['review_status']!='pending' or after['retained']!=before['retained']
                        or after['minimum_rights_version']<=creation_rights_version):
                    raise ValueError('Restore metadata replay evidence conflicts')
                found=True
            previous=after;next_version+=1
        if not found or next_version!=version+1 or previous!=current:
            raise ValueError('Restore metadata replay evidence conflicts')
    except (KeyError,TypeError,json.JSONDecodeError):
        raise ValueError('Restore metadata replay evidence conflicts') from None


def reconcile_metadata_restore_in(db, *, snapshot_set_id, snapshot_sha256, observed_at_ms):
    if not isinstance(snapshot_set_id,str) or not snapshot_set_id or not isinstance(snapshot_sha256,str) or len(snapshot_sha256)!=64 or any(x not in '0123456789abcdef' for x in snapshot_sha256) or type(observed_at_ms) is not int or observed_at_ms<0:
        raise ValueError('Fixed restore snapshot identity required')
    changed=0
    rows=db.execute("""SELECT k.candidate_id,k.version,k.minimum_rights_version,r.version,c.rights_version_at_creation FROM metadata_candidate_control k
      JOIN metadata_candidates c ON c.id=k.candidate_id JOIN metadata_rights r ON r.id=c.rights_id
      WHERE c.origin='PUBLISHER_OFFICIAL' ORDER BY k.candidate_id""").fetchall()
    for candidate,version,minimum,rights_version,creation_rights_version in rows:
        request=str(uuid.uuid5(uuid.NAMESPACE_URL,'restore-metadata:'+snapshot_set_id+':'+snapshot_sha256+':'+candidate))
        old=db.execute("SELECT id FROM metadata_control_events WHERE candidate_id=? AND request_id=? AND action='system_restore'",(candidate,request)).fetchone()
        if old:
            _verify_restore_replay_in(db,candidate,version,creation_rights_version,request,snapshot_sha256)
            continue
        count=db.execute("""UPDATE metadata_candidate_control SET minimum_rights_version=?,review_status='pending',version=version+1,
          actor_kind='system_restore',actor_id=NULL,request_id=?,updated_at_ms=?,source_snapshot_sha256=?,audit_context_json='{}'
          WHERE candidate_id=? AND version=?""",(max(minimum,rights_version+1),request,observed_at_ms,snapshot_sha256,candidate,version)).rowcount
        if count!=1:
            raise ValueError('Restore metadata control conflict')
        changed+=1
    return dict(metadata_official_revalidated=changed)
