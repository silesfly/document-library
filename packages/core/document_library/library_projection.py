"""Versioned synchronous library derivatives; source options remain authoritative.

The v1 SQL is migration0015's frozen contract. UDFs are pure and never reenter
SQLite. Refreshes run inside the source writer; callers own the transaction.
"""

AUTHORIZED_OPTIONS_V1 = """
 SELECT e.work_id,e.id AS edition_id,e.edition_key,e.representation,e.source_form,e.edition_label,e.language,
        r.id AS revision_id,s.sealed_at,opt.id AS option_id,opt.format,opt.delivery_kind,opt.coverage_json,opt.coverage_confidence
 FROM work_editions e JOIN edition_revisions r ON r.edition_id=e.id AND r.is_active=1
 JOIN revision_seals s ON s.revision_id=r.id
 JOIN publication_plans root INDEXED BY ix_library_allowed_plan_v1
   ON root.job_id=s.source_job_id AND root.generation=s.source_generation
 JOIN revision_options opt ON opt.edition_revision_id=r.id
 WHERE root.rights_status='allowed' AND library_plan_valid_v1(root.payload_json,root.payload_digest)=1
 AND EXISTS(SELECT 1 FROM option_assets member WHERE member.option_id=opt.id)
 AND NOT EXISTS(
  SELECT 1 FROM option_assets member
  LEFT JOIN revision_assets a ON a.edition_revision_id=member.edition_revision_id AND a.asset_position=member.asset_position
  LEFT JOIN objects o ON o.sha256=a.object_sha
  WHERE member.option_id=opt.id AND (
   a.object_sha IS NULL OR member.edition_revision_id<>r.id OR a.file_format<>opt.format
   OR o.sha256 IS NULL OR o.state<>'LIVE'
   OR o.local_relpath<>'objects/'||substr(o.sha256,1,2)||'/'||substr(o.sha256,3,2)||'/'||o.sha256
   OR NOT EXISTS(SELECT 1 FROM revision_asset_sources src WHERE src.edition_revision_id=a.edition_revision_id AND src.asset_position=a.asset_position)
   OR EXISTS(SELECT 1 FROM revision_asset_sources src
    WHERE src.edition_revision_id=a.edition_revision_id AND src.asset_position=a.asset_position
      AND NOT EXISTS(SELECT 1 FROM publication_plans p INDEXED BY ix_library_allowed_plan_v1
        WHERE p.job_id=src.source_job_id AND p.generation=src.source_generation
          AND p.rights_status='allowed' AND library_plan_valid_v1(p.payload_json,p.payload_digest)=1))
  ))
"""
VALID_OPTIONS_V1 = 'valid_options AS NOT MATERIALIZED (SELECT * FROM library_authorized_options_v1)'
EDITION_ORDER_V1 = "CASE representation WHEN 'text' THEN 0 WHEN 'imagepack' THEN 1 WHEN 'document' THEN 2 ELSE 3 END,edition_key,edition_id"
OPTION_ORDER_V1 = "CASE format WHEN 'txt' THEN 0 WHEN 'epub' THEN 1 WHEN 'pdf' THEN 2 ELSE 3 END,format,option_id"
DEFAULT_ORDER_V1 = EDITION_ORDER_V1+','+OPTION_ORDER_V1
WORK_COLUMNS_V1 = ('work_id','visible','basis_option_id','basis_revision_id','latest_accessible_seal',
                   'created_key','updated_key','episode_count','gaps_key','size_bytes','representation_mask','has_scan','formats_ready')
READINESS_V1 = """SELECT 1 FROM works w
 LEFT JOIN library_read_works p ON p.work_id=w.id
 LEFT JOIN library_rating_stats r ON r.work_id=w.id
 WHERE p.work_id IS NULL OR r.work_id IS NULL OR p.input_epoch<>p.computed_epoch OR p.formats_ready<>1 LIMIT 1"""


def expected_works_v1(work_scope='SELECT id FROM works'):
    """The same source derivation serves set-based rebuild and one-work refresh."""
    return """WITH scope(work_id) AS MATERIALIZED ("""+work_scope+"""),
 options AS MATERIALIZED (SELECT v.* FROM library_authorized_options_v1 v JOIN scope ON scope.work_id=v.work_id),
 ranked AS (SELECT *,row_number() OVER(PARTITION BY work_id ORDER BY """+DEFAULT_ORDER_V1+""") AS basis_rank FROM options),
 facts AS (SELECT work_id,max(sealed_at) AS latest_accessible_seal,
   max(CASE WHEN representation='text' THEN 1 ELSE 0 END)
   +max(CASE WHEN representation='imagepack' THEN 2 ELSE 0 END)
   +max(CASE WHEN representation='document' THEN 4 ELSE 0 END)
   +max(CASE WHEN representation='single_image' THEN 8 ELSE 0 END) AS representation_mask,
   max(source_form='scan') AS has_scan FROM options GROUP BY work_id)
 SELECT w.id AS work_id,CAST(basis.option_id IS NOT NULL AS INTEGER) AS visible,
 basis.option_id AS basis_option_id,basis.revision_id AS basis_revision_id,f.latest_accessible_seal,
 library_time_ms_v1(w.created_at) AS created_key,
 (SELECT max(value) FROM (SELECT library_time_ms_v1(w.updated_at) AS value
   UNION ALL SELECT library_time_ms_v1(m.updated_at) UNION ALL SELECT f.latest_accessible_seal)) AS updated_key,
 library_episode_count_v1(basis.coverage_json) AS episode_count,
 library_episode_gaps_v1(basis.coverage_json) AS gaps_key,
 (SELECT sum(o.byte_size) FROM option_assets oa JOIN revision_assets a
   ON a.edition_revision_id=oa.edition_revision_id AND a.asset_position=oa.asset_position
   JOIN objects o ON o.sha256=a.object_sha WHERE oa.option_id=basis.option_id) AS size_bytes,
 coalesce(f.representation_mask,0) AS representation_mask,coalesce(f.has_scan,0) AS has_scan,1 AS formats_ready
 FROM scope JOIN works w ON w.id=scope.work_id LEFT JOIN library_metadata m ON m.work_id=w.id
 LEFT JOIN ranked basis ON basis.work_id=w.id AND basis.basis_rank=1 LEFT JOIN facts f ON f.work_id=w.id"""


RATING_EXPECTED_V1 = """SELECT w.id,count(r.user_id),coalesce(sum(r.rating_half_steps),0)
 FROM works w LEFT JOIN ratings r ON r.work_id=w.id GROUP BY w.id"""
FORMAT_EXPECTED_V1 = 'SELECT DISTINCT work_id,format FROM library_authorized_options_v1'


def verify_projection_in(db):
    """Bidirectional parity uses authoritative sources, never cached values."""
    columns=','.join(WORK_COLUMNS_V1)
    for name,expected,actual in (
        ('works',expected_works_v1(),'SELECT '+columns+' FROM library_read_works'),
        ('formats',FORMAT_EXPECTED_V1,'SELECT work_id,format FROM library_read_formats'),
        ('ratings',RATING_EXPECTED_V1,'SELECT work_id,votes,half_sum FROM library_rating_stats'),
    ):
        row=db.execute('WITH expected AS ('+expected+'), actual AS ('+actual+''')
            SELECT 1 FROM (SELECT * FROM expected EXCEPT SELECT * FROM actual)
            UNION ALL SELECT 1 FROM (SELECT * FROM actual EXCEPT SELECT * FROM expected) LIMIT 1''').fetchone()
        if row:
            raise ValueError('Library projection parity differs: '+name)
    if db.execute(READINESS_V1).fetchone():
        raise ValueError('Library projection is not ready')


def rebuild_projection_in(db):
    """Caller holds one writer transaction. No trigger-driven per-work backfill."""
    db.execute('DELETE FROM library_read_formats')
    db.execute('DELETE FROM library_read_works')
    db.execute('DELETE FROM library_rating_stats')
    db.execute('INSERT INTO library_read_works('+','.join(WORK_COLUMNS_V1)+') '+expected_works_v1())
    db.execute('INSERT INTO library_read_formats '+FORMAT_EXPECTED_V1)
    db.execute('INSERT INTO library_rating_stats '+RATING_EXPECTED_V1)
    db.execute('UPDATE library_read_works SET formats_ready=1')
    db.execute('REINDEX ix_library_allowed_plan_v1')
    verify_projection_in(db)


def source_triggers_v1():
    """Emit bounded indexed OLD/NEW fanout. Frozen identifiers only."""
    statements=[]
    def bump(select):
        return 'UPDATE library_read_works SET input_epoch=input_epoch+1 WHERE work_id IN ('+select+');'
    def trigger(name,event,table,body,when=''):
        statements.append('CREATE TRIGGER library_projection_'+name+' AFTER '+event+' ON '+table+
                          (' WHEN '+when if when else '')+' BEGIN '+body+' END')
    # Epoch changes synchronously refresh the source-authorized work and formats.
    fields=','.join(WORK_COLUMNS_V1[1:])
    scoped=expected_works_v1('SELECT NEW.work_id')
    trigger('refresh','UPDATE OF input_epoch','library_read_works',
        'DELETE FROM library_read_formats WHERE work_id=NEW.work_id;'
        'INSERT INTO library_read_formats SELECT DISTINCT work_id,format FROM library_authorized_options_v1 WHERE work_id=NEW.work_id;'
        'UPDATE library_read_works SET ('+fields+')=(SELECT '+fields+' FROM ('+scoped+')),'
        'computed_epoch=NEW.input_epoch WHERE work_id=NEW.work_id;', 'NEW.input_epoch<>NEW.computed_epoch')
    trigger('work_insert','INSERT','works',
        'INSERT INTO library_read_works(work_id) VALUES(NEW.id);'
        'INSERT INTO library_rating_stats(work_id,votes,half_sum) SELECT NEW.id,count(*),coalesce(sum(rating_half_steps),0) FROM ratings WHERE work_id=NEW.id;'+bump('SELECT NEW.id'))
    trigger('work_time','UPDATE OF created_at,updated_at','works',bump('SELECT NEW.id'))
    for event,aliases in (('INSERT',('NEW',)),('UPDATE',('OLD','NEW')),('DELETE',('OLD',))):
        suffix=event.lower()
        trigger('metadata_'+suffix,event,'library_metadata',bump(' UNION '.join('SELECT '+a+'.work_id' for a in aliases)))
        # Normal draft insertion has no sealed children. A REPLACE with the
        # same edition identity can inherit them: its NEW owner must refresh.
        trigger('edition_'+suffix,event,'work_editions',
            bump(' UNION '.join('SELECT '+a+'.work_id' for a in aliases)),
            "EXISTS(SELECT 1 FROM edition_revisions r JOIN revision_seals s ON s.revision_id=r.id WHERE r.edition_id=NEW.id)" if event=='INSERT' else '')
        trigger('format_dirty_'+suffix,event,'library_read_formats',
            'UPDATE library_read_works SET formats_ready=0 WHERE work_id IN ('+
            ' UNION '.join('SELECT '+a+'.work_id' for a in aliases)+');')
        trigger('revision_'+suffix,event,'edition_revisions',bump(' UNION '.join(
            'SELECT work_id FROM work_editions WHERE id='+a+'.edition_id' for a in aliases)),
            ' OR '.join('EXISTS(SELECT 1 FROM revision_seals WHERE revision_id='+a+'.id)' for a in aliases))
        trigger('seal_'+suffix,event,'revision_seals',bump(' UNION '.join(
            'SELECT e.work_id FROM edition_revisions r JOIN work_editions e ON e.id=r.edition_id WHERE r.id='+a+'.revision_id' for a in aliases)))
        # Root seals AND inherited per-asset source bindings are authority inputs.
        trigger('plan_'+suffix,event,'publication_plans',bump(' UNION '.join(
            'SELECT e.work_id FROM revision_seals s JOIN edition_revisions r ON r.id=s.revision_id JOIN work_editions e ON e.id=r.edition_id WHERE s.source_job_id='+a+'.job_id AND s.source_generation='+a+'.generation'
            ' UNION SELECT e.work_id FROM revision_asset_sources s JOIN edition_revisions r ON r.id=s.edition_revision_id JOIN work_editions e ON e.id=r.edition_id WHERE s.source_job_id='+a+'.job_id AND s.source_generation='+a+'.generation' for a in aliases)))
        trigger('object_'+suffix,event,'objects',bump(' UNION '.join(
            'SELECT e.work_id FROM revision_assets a JOIN edition_revisions r ON r.id=a.edition_revision_id JOIN work_editions e ON e.id=r.edition_id WHERE a.object_sha='+a+'.sha256' for a in aliases)))
        for table in ('revision_assets','revision_options','option_assets','revision_asset_sources'):
            trigger(table+'_'+suffix,event,table,bump(' UNION '.join(
                'SELECT e.work_id FROM edition_revisions r JOIN work_editions e ON e.id=r.edition_id WHERE r.id='+a+'.edition_revision_id' for a in aliases)),
                ' OR '.join('EXISTS(SELECT 1 FROM revision_seals WHERE revision_id='+a+'.edition_revision_id)' for a in aliases))
        # Add/subtract integers for OLD and NEW work. User status is irrelevant.
        body=''
        if 'OLD' in aliases:
            body+='UPDATE library_rating_stats SET votes=votes-1,half_sum=half_sum-OLD.rating_half_steps WHERE work_id=OLD.work_id;'
        if 'NEW' in aliases:
            body+='UPDATE library_rating_stats SET votes=votes+1,half_sum=half_sum+NEW.rating_half_steps WHERE work_id=NEW.work_id;'
        trigger('rating_'+suffix,event,'ratings',body)
    source_tables=('works','work_editions','edition_revisions','revision_seals','publication_plans','objects',
                   'revision_assets','revision_options','option_assets','revision_asset_sources','library_metadata','ratings')
    for table in source_tables:
        for event in ('INSERT','UPDATE','DELETE'):
            statements.append('CREATE TRIGGER library_recursive_'+table+'_'+event.lower()+' BEFORE '+event+' ON '+table+
                " WHEN (SELECT recursive_triggers FROM pragma_recursive_triggers)<>1 BEGIN SELECT RAISE(ABORT,'recursive library triggers required'); END")
    # UPDATE guards already freeze identities, but REPLACE performs DELETE.
    # Source plans/objects are retained metadata; lifecycle purge changes state.
    for table in ('publication_plans','objects'):
        statements.append('CREATE TRIGGER library_identity_'+table+'_delete BEFORE DELETE ON '+table+
                          " BEGIN SELECT RAISE(ABORT,'library source identity is immutable'); END")
    return statements
