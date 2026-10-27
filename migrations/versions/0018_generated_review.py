"""Immutable provenance and removable auxiliary payloads for generated review."""
from alembic import op

revision = '0018_generated_review'
down_revision = '0017_metadata_publication'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""CREATE TABLE metadata_generated_bindings (
      candidate_id TEXT PRIMARY KEY NOT NULL REFERENCES metadata_candidates(id),
      schema_version TEXT NOT NULL CHECK(schema_version='metadata.generated.binding.v1'),
      binding_json TEXT NOT NULL CHECK(json_valid(binding_json) AND json_type(binding_json)='object' AND length(CAST(binding_json AS BLOB))<=2000000),
      binding_digest TEXT NOT NULL CHECK(length(binding_digest)=64 AND binding_digest NOT GLOB '*[^0-9a-f]*'),
      review_unit_id TEXT NOT NULL CHECK(length(review_unit_id)=75 AND substr(review_unit_id,1,11)='summary-v1:' AND substr(review_unit_id,12) NOT GLOB '*[^0-9a-f]*'),
      summary_codepoints INTEGER NOT NULL CHECK(summary_codepoints BETWEEN 1 AND 16384),
      created_at_ms INTEGER NOT NULL CHECK(created_at_ms>=0)
    ) STRICT""")
    op.execute("""CREATE TABLE metadata_generated_payloads (
      candidate_id TEXT PRIMARY KEY NOT NULL REFERENCES metadata_candidates(id),
      content_sha256 TEXT NOT NULL CHECK(length(content_sha256)=64 AND content_sha256 NOT GLOB '*[^0-9a-f]*'),
      auxiliary_json TEXT NOT NULL CHECK(json_valid(auxiliary_json) AND json_type(auxiliary_json)='object' AND length(CAST(auxiliary_json AS BLOB))<=32768),
      auxiliary_digest TEXT NOT NULL CHECK(length(auxiliary_digest)=64 AND auxiliary_digest NOT GLOB '*[^0-9a-f]*'),
      created_at_ms INTEGER NOT NULL CHECK(created_at_ms>=0)
    ) STRICT""")
    # The request digest is part of canonical binding JSON. The separate
    # binding_digest covers the complete immutable record and is checked by the
    # service when the row is created and by the backup verifier thereafter.
    op.execute("""CREATE TRIGGER metadata_generated_binding_insert_guard
      BEFORE INSERT ON metadata_generated_bindings
      WHEN NOT EXISTS(
        SELECT 1 FROM metadata_candidates c
        JOIN metadata_job_inputs i ON i.job_id=c.producing_job_id
        JOIN jobs j ON j.id=c.producing_job_id
        WHERE c.id=NEW.candidate_id AND c.kind='description' AND c.origin='QWEN_TEXT'
          AND c.producing_job_id IS NOT NULL AND c.generation IS NOT NULL
          AND json_extract(NEW.binding_json,'$.schema')='metadata.generated.binding.v1'
          AND json_extract(NEW.binding_json,'$.candidate_id')=c.id
          AND json_extract(NEW.binding_json,'$.job_id')=c.producing_job_id
          AND json_extract(NEW.binding_json,'$.generation')=c.generation
          AND json_extract(NEW.binding_json,'$.work_id')=c.work_id
          AND json_extract(NEW.binding_json,'$.source_revision_id')=c.source_revision_id
          AND json_extract(NEW.binding_json,'$.rights_id')=c.rights_id
          AND json_extract(NEW.binding_json,'$.generation_rights_version')=c.rights_version_at_creation
          AND json_extract(NEW.binding_json,'$.request_digest')=i.request_digest
          AND i.origin='QWEN_TEXT' AND i.work_id=c.work_id AND i.source_revision_id=c.source_revision_id
          AND i.rights_id=c.rights_id AND i.expected_rights_version=c.rights_version_at_creation
          AND j.kind='metadata.description.publish' AND j.work_id=c.work_id
      ) BEGIN SELECT RAISE(ABORT,'generated binding does not match producer'); END""")
    op.execute("""CREATE TRIGGER metadata_generated_binding_update_guard
      BEFORE UPDATE ON metadata_generated_bindings
      BEGIN SELECT RAISE(ABORT,'generated binding is immutable'); END""")
    op.execute("""CREATE TRIGGER metadata_generated_binding_delete_guard
      BEFORE DELETE ON metadata_generated_bindings
      BEGIN SELECT RAISE(ABORT,'generated binding is immutable'); END""")
    op.execute("""CREATE TRIGGER metadata_generated_payload_insert_guard
      BEFORE INSERT ON metadata_generated_payloads
      WHEN NOT EXISTS(
        SELECT 1 FROM metadata_generated_bindings b
        JOIN metadata_candidates c ON c.id=b.candidate_id
        JOIN metadata_payloads p ON p.candidate_id=c.id
        WHERE c.id=NEW.candidate_id AND c.origin='QWEN_TEXT' AND c.kind='description'
          AND c.content_sha256=NEW.content_sha256 AND p.text_sha256=NEW.content_sha256
          AND NOT EXISTS(SELECT 1 FROM metadata_retention_tombstones t WHERE t.candidate_id=c.id)
          AND json_extract(NEW.auxiliary_json,'$.schema')='metadata.generated.auxiliary.v1'
      ) BEGIN SELECT RAISE(ABORT,'generated auxiliary payload lacks current candidate'); END""")
    op.execute("""CREATE TRIGGER metadata_generated_payload_update_guard
      BEFORE UPDATE ON metadata_generated_payloads
      BEGIN SELECT RAISE(ABORT,'generated auxiliary payload is immutable until deletion'); END""")
    op.execute("""CREATE TRIGGER metadata_generated_payload_delete_after_payload
      AFTER DELETE ON metadata_payloads
      BEGIN DELETE FROM metadata_generated_payloads WHERE candidate_id=OLD.candidate_id; END""")
    op.execute("""CREATE TRIGGER metadata_generated_payload_delete_after_tombstone
      AFTER INSERT ON metadata_retention_tombstones
      BEGIN DELETE FROM metadata_generated_payloads WHERE candidate_id=NEW.candidate_id; END""")
    # Generic candidate review remains valid for legacy, editorial and official
    # candidates. A new generated approval needs the typed human-review receipt.
    # Generated controls are created pending, then moved to approved through
    # the guarded UPDATE below. An INSERT has no prior pending control/version
    # to compare and therefore cannot be a valid approval transition.
    op.execute("""CREATE TRIGGER metadata_generated_approval_guard_insert
      BEFORE INSERT ON metadata_candidate_control
      WHEN NEW.review_status='approved' AND EXISTS(
        SELECT 1 FROM metadata_candidates c WHERE c.id=NEW.candidate_id AND c.origin IN ('QWEN_TEXT','QWEN_OCR')
      ) BEGIN SELECT RAISE(ABORT,'generated approval requires an existing pending control'); END""")
    op.execute("""CREATE TRIGGER metadata_generated_approval_guard_update
      BEFORE UPDATE ON metadata_candidate_control
      WHEN NEW.review_status='approved' AND EXISTS(
        SELECT 1 FROM metadata_candidates c WHERE c.id=NEW.candidate_id AND c.origin IN ('QWEN_TEXT','QWEN_OCR')
      ) AND NOT EXISTS(
        SELECT 1 FROM metadata_generated_bindings b
        JOIN metadata_candidates c ON c.id=b.candidate_id
        JOIN metadata_payloads p ON p.candidate_id=c.id
        WHERE c.id=NEW.candidate_id AND c.origin='QWEN_TEXT'
          AND c.content_sha256=json_extract(b.binding_json,'$.final_output_sha256')
          AND p.text_sha256=c.content_sha256
          AND length(p.text_value)=b.summary_codepoints
          AND NEW.actor_kind='admin' AND NEW.actor_id IS NOT NULL AND length(NEW.actor_id)>0
          AND NEW.version=OLD.version+1
          AND json_type(NEW.audit_context_json) IS 'object'
          AND json_extract(NEW.audit_context_json,'$.operation') IS 'generated_human_review_v1'
          AND json_extract(NEW.audit_context_json,'$.candidate_id') IS c.id
          AND json_extract(NEW.audit_context_json,'$.binding_digest') IS b.binding_digest
          AND json_type(NEW.audit_context_json,'$.expected_control_version') IS 'integer'
          AND json_extract(NEW.audit_context_json,'$.expected_control_version')=OLD.version
          AND json_type(NEW.audit_context_json,'$.expected_head_version') IS 'integer'
          AND json_extract(NEW.audit_context_json,'$.expected_head_version')>=0
          AND json_type(NEW.audit_context_json,'$.expected_rights_version') IS 'integer'
          AND json_extract(NEW.audit_context_json,'$.expected_rights_version')>0
          AND json_type(NEW.audit_context_json,'$.review_context_digest') IS 'text'
          AND length(json_extract(NEW.audit_context_json,'$.review_context_digest'))=64
          AND json_extract(NEW.audit_context_json,'$.review_context_digest') NOT GLOB '*[^0-9a-f]*'
          AND json_type(NEW.audit_context_json,'$.decision_digest') IS 'text'
          AND length(json_extract(NEW.audit_context_json,'$.decision_digest'))=64
          AND json_extract(NEW.audit_context_json,'$.decision_digest') NOT GLOB '*[^0-9a-f]*'
          AND json_type(NEW.audit_context_json,'$.operation_digest') IS 'text'
          AND length(json_extract(NEW.audit_context_json,'$.operation_digest'))=64
          AND json_extract(NEW.audit_context_json,'$.operation_digest') NOT GLOB '*[^0-9a-f]*'
          AND json_extract(NEW.audit_context_json,'$.semantic_review') IS 'HUMAN_ADMIN_ATTESTED'
          AND json_extract(NEW.audit_context_json,'$.decision') IS 'approved'
          AND json_extract(NEW.audit_context_json,'$.grounding') IS 'grounded'
          AND json_type(NEW.audit_context_json,'$.spoiler') IS 'text'
          AND json_type(NEW.audit_context_json,'$.select') IN ('true','false')
          AND json_type(NEW.audit_context_json,'$.review_units') IS 'array'
          AND json_array_length(NEW.audit_context_json,'$.review_units')=1
          AND json_type(NEW.audit_context_json,'$.review_units[0]') IS 'object'
          AND (SELECT count(*) FROM json_each(NEW.audit_context_json,'$.review_units[0]'))=2
          AND json_extract(NEW.audit_context_json,'$.review_units[0].unit_id') IS b.review_unit_id
          AND json_type(NEW.audit_context_json,'$.review_units[0].range_indexes') IS 'array'
          AND json_array_length(NEW.audit_context_json,'$.review_units[0].range_indexes') BETWEEN 1 AND 16
          AND (SELECT count(*) FROM json_each(NEW.audit_context_json,'$.review_units[0].range_indexes'))=
              (SELECT count(DISTINCT value) FROM json_each(NEW.audit_context_json,'$.review_units[0].range_indexes'))
          AND json_type(NEW.audit_context_json,'$.ranges') IS 'array'
          AND json_array_length(NEW.audit_context_json,'$.ranges') BETWEEN 1 AND 16
          AND NOT EXISTS (
            SELECT 1 FROM json_each(NEW.audit_context_json,'$.review_units[0].range_indexes') i
            WHERE i.type IS NOT 'integer' OR i.value<0
              OR i.value>=json_array_length(NEW.audit_context_json,'$.ranges')
              OR NOT EXISTS(
                SELECT 1 FROM json_each(NEW.audit_context_json,'$.ranges') r
                WHERE CAST(r.key AS INTEGER)=i.value
                  AND json_type(r.value,'$.tail') IS 'false'
              )
          )
          AND NOT EXISTS (
            SELECT 1 FROM json_each(NEW.audit_context_json,'$.ranges') r
            WHERE r.type IS NOT 'object'
              OR (SELECT count(*) FROM json_each(r.value))!=4
              OR json_type(r.value,'$.start') IS NOT 'integer'
              OR json_type(r.value,'$.end') IS NOT 'integer'
              OR json_extract(r.value,'$.start')<0
              OR json_extract(r.value,'$.end')<=json_extract(r.value,'$.start')
              OR json_extract(r.value,'$.end')>json_extract(b.binding_json,'$.full_text_codepoints')
              OR json_extract(r.value,'$.end')-json_extract(r.value,'$.start')>2048
              OR json_type(r.value,'$.sha256') IS NOT 'text'
              OR length(json_extract(r.value,'$.sha256'))!=64
              OR json_extract(r.value,'$.sha256') GLOB '*[^0-9a-f]*'
              OR (json_type(r.value,'$.tail') IS NOT 'true'
                  AND json_type(r.value,'$.tail') IS NOT 'false')
              OR (json_extract(r.value,'$.tail')=1
                  AND (json_extract(r.value,'$.start')<json_extract(b.binding_json,'$.excluded_tail_range.start')
                    OR json_extract(r.value,'$.end')>json_extract(b.binding_json,'$.excluded_tail_range.end')
                    OR json_extract(r.value,'$.end')<=json_extract(b.binding_json,'$.excluded_tail_range.start')))
              OR (json_extract(r.value,'$.tail')=0
                  AND json_extract(r.value,'$.end')>json_extract(b.binding_json,'$.excluded_tail_range.start'))
          )
          AND json_type(NEW.audit_context_json,'$.flags') IS 'array'
          AND (SELECT count(*) FROM json_each(NEW.audit_context_json,'$.flags'))=
              (SELECT count(*) FROM json_each(b.binding_json,'$.qa_codes') q
                WHERE q.type IS 'text'
                  AND q.value NOT IN ('SEMANTIC_GROUNDING_NOT_VERIFIED','SPOILER_SEMANTICS_NOT_VERIFIED'))
          AND NOT EXISTS(
            SELECT 1 FROM json_each(b.binding_json,'$.qa_codes') q
            WHERE q.type IS NOT 'text'
              OR (q.value NOT IN ('SEMANTIC_GROUNDING_NOT_VERIFIED','SPOILER_SEMANTICS_NOT_VERIFIED')
                AND NOT EXISTS(
                  SELECT 1 FROM json_each(NEW.audit_context_json,'$.flags') f
                  WHERE json_extract(f.value,'$.code')=q.value
                    AND json_extract(f.value,'$.resolution')='accept_with_evidence'
                ))
          )
          AND NOT EXISTS(
            SELECT 1 FROM json_each(NEW.audit_context_json,'$.flags') f
            WHERE f.type IS NOT 'object'
              OR (SELECT count(*) FROM json_each(f.value))!=3
              OR json_type(f.value,'$.code') IS NOT 'text'
              OR json_extract(f.value,'$.code') NOT IN (
                SELECT q.value FROM json_each(b.binding_json,'$.qa_codes') q
                WHERE q.type IS 'text'
                  AND q.value NOT IN ('SEMANTIC_GROUNDING_NOT_VERIFIED','SPOILER_SEMANTICS_NOT_VERIFIED'))
              OR json_extract(f.value,'$.resolution') IS NOT 'accept_with_evidence'
              OR json_type(f.value,'$.range_indexes') IS NOT 'array'
              OR json_array_length(f.value,'$.range_indexes') NOT BETWEEN 1 AND 16
              OR EXISTS(
                SELECT 1 FROM json_each(f.value,'$.range_indexes') i
                WHERE i.type IS NOT 'integer' OR i.value<0
                  OR i.value>=json_array_length(NEW.audit_context_json,'$.ranges'))
              OR (SELECT count(*) FROM json_each(f.value,'$.range_indexes'))!=
                 (SELECT count(DISTINCT value) FROM json_each(f.value,'$.range_indexes'))
          )
          AND (
            (json_extract(b.binding_json,'$.item_type')='novel'
              AND json_extract(NEW.audit_context_json,'$.spoiler') IS 'acceptable'
              AND EXISTS(SELECT 1 FROM json_each(NEW.audit_context_json,'$.ranges') t
                         WHERE json_type(t.value,'$.tail') IS 'true'))
            OR (json_extract(b.binding_json,'$.item_type')='document'
              AND json_extract(NEW.audit_context_json,'$.spoiler') IN ('acceptable','not_applicable'))
          )
      ) BEGIN SELECT RAISE(ABORT,'generated approval requires human-review evidence'); END""")
    op.execute("""CREATE TRIGGER metadata_generated_pending_guard_insert
      BEFORE INSERT ON metadata_candidate_control
      WHEN NEW.review_status='pending' AND EXISTS(
        SELECT 1 FROM metadata_candidates c WHERE c.id=NEW.candidate_id AND c.origin IN ('QWEN_TEXT','QWEN_OCR')
      ) AND NOT EXISTS(
        SELECT 1 FROM metadata_generated_bindings b JOIN metadata_candidates c ON c.id=b.candidate_id
        WHERE c.id=NEW.candidate_id AND c.origin='QWEN_TEXT' AND c.content_sha256=json_extract(b.binding_json,'$.final_output_sha256')
      ) BEGIN SELECT RAISE(ABORT,'generated pending candidate requires typed binding'); END""")
    op.execute("""CREATE TRIGGER metadata_generated_head_guard_insert
      BEFORE INSERT ON metadata_heads
      WHEN NEW.description_id IS NOT NULL AND EXISTS(
        SELECT 1 FROM metadata_candidates c WHERE c.id=NEW.description_id AND c.origin IN ('QWEN_TEXT','QWEN_OCR')
      ) AND NOT EXISTS(
        SELECT 1 FROM metadata_candidate_control k
        WHERE k.candidate_id=NEW.description_id AND k.review_status='approved'
          AND json_extract(k.audit_context_json,'$.operation')='generated_human_review_v1'
      ) BEGIN SELECT RAISE(ABORT,'generated head requires human-review evidence'); END""")
    op.execute("""CREATE TRIGGER metadata_generated_head_guard_update
      BEFORE UPDATE ON metadata_heads
      WHEN NEW.description_id IS NOT OLD.description_id AND NEW.description_id IS NOT NULL AND EXISTS(
        SELECT 1 FROM metadata_candidates c WHERE c.id=NEW.description_id AND c.origin IN ('QWEN_TEXT','QWEN_OCR')
      ) AND NOT EXISTS(
        SELECT 1 FROM metadata_candidate_control k
        WHERE k.candidate_id=NEW.description_id AND k.review_status='approved'
          AND json_extract(k.audit_context_json,'$.operation')='generated_human_review_v1'
      ) BEGIN SELECT RAISE(ABORT,'generated head requires human-review evidence'); END""")


def downgrade():
    raise RuntimeError('Destructive downgrade is not enabled')
