"""Bounded G2A policy candidate authored by sol_engineering.

Candidate only: canonical apply, original duplication baseline, native/fallback
receipts and runtime validation belong to the G2A integration writer. This author
is not the independent final reviewer. All identities/objects are disposable.
"""
from dataclasses import FrozenInstanceError, replace
import json
import unittest

from sqlalchemy import event, text

from document_library.auth import AuthError, digest
from document_library.database import write_transaction
from document_library.library_contracts import AssetSelection, TICKET_TTL_MS
from document_library.library_policy import LibraryPolicyError, LibraryReadPolicy
from library_helpers import LibraryFixture
import test_ingestion


class LibraryPolicyAcceptance(LibraryFixture, unittest.TestCase):
    def test_ticket_millisecond_clock_exact_edges_and_invalid_values(self):
        revision, _lease, _asset = self.first()
        self.h.clock.advance(123)
        ticket = self.ticket(revision)
        with self.engine.connect() as conn:
            self.assertEqual(self.policy.load_ticket_snapshot_in(conn,self.token,ticket.ticket_id),ticket)
        self.h.clock.advance(119999)
        with self.engine.connect() as conn:
            self.policy.load_ticket_snapshot_in(conn,self.token,ticket.ticket_id)
        self.h.clock.advance(1)
        with self.engine.connect() as conn,self.assertRaises(LibraryPolicyError) as caught:
            self.policy.load_ticket_snapshot_in(conn,self.token,ticket.ticket_id)
        self.assertEqual(caught.exception.code,'TICKET_EXPIRED_OR_USED')
        for value in (None,True,-1,1.0,'1800000000000'):
            policy=LibraryReadPolicy(self.auth,now_ms=lambda:value)
            with self.subTest(value=value),self.engine.connect() as conn,self.assertRaises(LibraryPolicyError) as caught:
                policy.load_ticket_snapshot_in(conn,self.token,ticket.ticket_id)
            self.assertEqual(caught.exception.code,'CLOCK_UNAVAILABLE')

    def resolve(self, selection, *, token=None, policy='current', ticket=None):
        with self.engine.connect() as conn:
            return self.policy.resolve_selection_in(conn, token or self.token, selection,
                                                     policy=policy, ticket_snapshot=ticket)

    def selection(self, revision, **kwargs):
        return (AssetSelection(revision, positions=(0,), **kwargs),)

    def ticket_selection(self, snapshot):
        return self.policy.selections_for_ticket(snapshot)

    def denied(self, selection, code, status, **kwargs):
        with self.assertRaises(AuthError) as caught:
            self.resolve(selection, **kwargs)
        self.assertEqual((caught.exception.code, caught.exception.status), (code, status))

    def test_current_selection_is_immutable_and_public_payload_hides_authority(self):
        revision, lease, asset = self.first()
        snapshot = self.resolve(self.selection(revision, work_id='w', edition_id='e'))
        current = snapshot.assets[0]
        self.assertEqual((current.revision_id, current.asset_position, current.object_sha,
                          current.file_format, current.byte_size),
                         (revision, 0, asset['sha256'], asset['file_format'], asset['byte_size']))
        self.assertEqual(current.sources[0].job_id, lease.job_id)
        self.assertEqual(snapshot.principal.session_hash, digest(self.token))
        with self.assertRaises(FrozenInstanceError):
            current.byte_size = 0
        with self.assertRaises(FrozenInstanceError):
            snapshot.assets = ()
        public = snapshot.public()
        encoded = json.dumps(public)
        for secret in (self.token, digest(self.token), lease.job_id, asset['prepared_relpath']):
            self.assertNotIn(secret, encoded)
        def assert_fields(value):
            if isinstance(value, dict):
                self.assertFalse(set(value) & {'principal', 'session_hash', 'job_id', 'sources', 'previews',
                                              'prepared_relpath', 'local_relpath', 'bundle_relpath', 'manifest_json'})
                for child in value.values():
                    assert_fields(child)
            elif isinstance(value, list):
                for child in value:
                    assert_fields(child)
        assert_fields(public)
        public['assets'][0]['byte_size'] = 0
        self.assertEqual(current.byte_size, asset['byte_size'])

    def test_current_active_transition_is_rechecked_and_retained_requires_admin(self):
        old, _lease, _asset = self.first()
        self.resolve(self.selection(old))
        latest, _new, _asset = self.publish(payload=b'new original', previous=old)
        self.denied(self.selection(old), 'RESOURCE_NOT_FOUND', 404)
        self.resolve(self.selection(latest))
        self.denied(self.selection(old), 'PERMISSION_DENIED', 403, policy='admin_retained')
        self.admin()
        self.resolve(self.selection(old), policy='admin_retained')
        with write_transaction(self.engine) as conn:
            conn.execute(text('UPDATE revision_seals SET retained=0 WHERE revision_id=:id'), dict(id=old))
        self.denied(self.selection(old), 'RESOURCE_NOT_FOUND', 404, policy='admin_retained')

    def test_selection_work_edition_option_and_missing_position_cannot_cross_bind(self):
        old, _lease, _asset = self.first()
        old_option = self.scalar('SELECT id FROM revision_options WHERE edition_revision_id=:id', id=old)
        latest, _new, _asset = self.publish(payload=b'different current bytes', previous=old)
        self.denied(self.selection(latest, work_id='another-work'), 'RESOURCE_NOT_FOUND', 404)
        self.denied(self.selection(latest, edition_id='another-edition'), 'RESOURCE_NOT_FOUND', 404)
        self.denied((AssetSelection(latest, option_id=old_option),), 'ASSET_UNAVAILABLE', 409)
        self.denied((AssetSelection(latest, positions=(999,)),), 'ASSET_UNAVAILABLE', 409)
        self.denied((), 'INVALID_SELECTION', 400)
        self.denied(self.selection(latest), 'INVALID_SELECTION', 400, policy='service')

    def test_sealed_option_format_must_match_its_actual_asset_format(self):
        # A trusted legacy publisher can contain such inconsistent metadata.
        # The G2 resolver must reject it, rather than advertise TXT as EPUB.
        lease = self.h.make_job()
        asset = self.h.asset(lease, b'actual TXT original', format='txt')
        coverage = dict(unit='unknown', intervals=[], gaps=[], confidence='unknown')
        self.h.plan(lease, assets=[asset], options=[dict(format='epub', delivery_kind='single_file',
                                                       positions=[0], coverage=coverage)])
        revision = self.publisher.publish(lease)
        option = self.scalar('SELECT id FROM revision_options WHERE edition_revision_id=:id', id=revision)
        with self.assertRaises(LibraryPolicyError):
            self.resolve((AssetSelection(revision, option_id=option),))
        with self.assertRaises(LibraryPolicyError):
            self.resolve((AssetSelection(revision, positions=(0,)),))

    def test_option_members_preserve_canonical_ordinal_order(self):
        lease = self.h.make_job()
        first = self.h.asset(lease, b'first raw original')
        second = self.h.asset(lease, b'second raw original')
        coverage = dict(unit='unknown', intervals=[], gaps=[], confidence='unknown')
        self.h.plan(lease, assets=[first, second], options=[dict(format='txt', delivery_kind='bundle',
                                                              positions=[1, 0], coverage=coverage)])
        revision = self.publisher.publish(lease)
        option = self.scalar('SELECT id FROM revision_options WHERE edition_revision_id=:id', id=revision)
        result = self.resolve((AssetSelection(revision, option_id=option),))
        self.assertEqual([a.asset_position for a in result.assets], [1, 0])
        self.assertEqual([a.ordinal for a in result.assets], [0, 1])
        self.assertEqual([a.object_sha for a in result.assets], [second['sha256'], first['sha256']])
        self.assertEqual(result.options[0].positions, (1, 0))
        self.denied((AssetSelection(revision, option_id=option, positions=(0, 1)),), 'INVALID_SELECTION', 400)

    def test_live_state_is_required_for_current_and_ticket_reads(self):
        revision, _lease, asset = self.first()
        ticket = self.ticket(revision)
        for state in ('MISSING', 'QUARANTINED', 'PURGED'):
            with self.subTest(state=state):
                with write_transaction(self.engine) as conn:
                    conn.execute(text('UPDATE objects SET state=:state WHERE sha256=:sha'),
                                 dict(state=state, sha=asset['sha256']))
                self.denied(self.selection(revision), 'ASSET_UNAVAILABLE', 409)
                self.denied(self.ticket_selection(ticket), 'ASSET_UNAVAILABLE', 409,
                            policy='ticket', ticket=ticket)

    def test_current_revision_source_rights_are_rechecked_after_prior_success(self):
        revision, lease, _asset = self.first()
        self.resolve(self.selection(revision))
        with write_transaction(self.engine) as conn:
            conn.execute(text("UPDATE publication_plans SET rights_status='blocked' WHERE job_id=:id"), dict(id=lease.job_id))
        self.denied(self.selection(revision), 'ASSET_ACCESS_DENIED', 403)

    def test_inherited_rights_cannot_be_reauthorized_by_new_revision_source(self):
        old, source, original = self.first()
        latest, fresh, new_asset = self.publish(payload=b'new unrelated source bytes', previous=old)
        inherited = self.scalar('SELECT asset_position FROM revision_assets WHERE edition_revision_id=:id AND object_sha=:sha',
                                id=latest, sha=original['sha256'])
        new_position = self.scalar('SELECT asset_position FROM revision_assets WHERE edition_revision_id=:id AND object_sha=:sha',
                                   id=latest, sha=new_asset['sha256'])
        self.resolve((AssetSelection(latest, positions=(inherited,)),))
        with write_transaction(self.engine) as conn:
            conn.execute(text("UPDATE publication_plans SET rights_status='blocked' WHERE job_id=:id"), dict(id=source.job_id))
        self.assertEqual(self.scalar('SELECT rights_status FROM publication_plans WHERE job_id=:id', id=fresh.job_id), 'allowed')
        self.denied((AssetSelection(latest, positions=(inherited,)),), 'ASSET_ACCESS_DENIED', 403)
        self.resolve((AssetSelection(latest, positions=(new_position,)),))

    def test_ticket_is_same_session_not_just_same_user_and_current_auth_is_required(self):
        revision, _lease, _asset = self.first()
        ticket = self.ticket(revision)
        selection = self.ticket_selection(ticket)
        self.resolve(selection, policy='ticket', ticket=ticket)
        other = 'synthetic-same-user-different-session'
        self.session(other)
        self.denied(selection, 'TICKET_ACCESS_DENIED', 403, token=other, policy='ticket', ticket=ticket)
        with write_transaction(self.engine) as conn:
            conn.execute(text('DELETE FROM auth_sessions WHERE token_hash=:hash'), dict(hash=digest(self.token)))
        self.denied(selection, 'authentication_required', 401, policy='ticket', ticket=ticket)
        self.assertEqual(self.scalar('SELECT count(*) FROM download_tickets WHERE id=:id', id=ticket.ticket_id), 1)
        self.assertGreater(self.scalar("SELECT count(*) FROM object_pins WHERE kind='ticket' AND reference_id=:id", id=ticket.ticket_id), 0)

    def test_ticket_owner_and_credential_binding_are_current(self):
        revision, _lease, _asset = self.first()
        ticket = self.ticket(revision)
        selection = self.ticket_selection(ticket)
        outsider = 'synthetic-other-user-session'
        with write_transaction(self.engine) as conn:
            conn.execute(text("INSERT INTO users(id,username,password_hash,created_at) VALUES('other-user','other-member','disabled-synthetic','test')"))
            conn.execute(text("INSERT INTO auth_sessions VALUES(:token,:csrf,'other-user',1,:now,:expiry)"),
                         dict(token=digest(outsider), csrf=digest('synthetic-other-csrf'), now=self.auth.now(), expiry=self.auth.now()+3600))
        self.denied(selection, 'TICKET_ACCESS_DENIED', 403, token=outsider, policy='ticket', ticket=ticket)
        with write_transaction(self.engine) as conn:
            conn.execute(text("UPDATE users SET credential_version=credential_version+1 WHERE id='u'"))
        self.denied(selection, 'authentication_required', 401, policy='ticket', ticket=ticket)
        refreshed = 'synthetic-after-credential-change'
        self.session(refreshed)
        self.denied(selection, 'TICKET_ACCESS_DENIED', 403, token=refreshed, policy='ticket', ticket=ticket)

    def test_issued_ticket_expires_but_matching_running_attempt_outlives_admission_ttl(self):
        revision, _lease, _asset = self.first()
        issued = self.ticket(revision)
        self.h.clock.advance(TICKET_TTL_MS+1000)
        self.denied(self.ticket_selection(issued), 'TICKET_EXPIRED_OR_USED', 409, policy='ticket', ticket=issued)
        # A separate ticket is claimed while fresh, then its physical attempt
        # remains running beyond TTL. Expiration is not a stream-stop signal.
        second = self.ticket(revision)
        active = self.claim(second)
        self.h.clock.advance(TICKET_TTL_MS+1000)
        self.resolve(self.ticket_selection(active), policy='ticket', ticket=active)
        self.denied(self.ticket_selection(second), 'TICKET_EXPIRED_OR_USED', 409, policy='ticket', ticket=second)
        forged_attempt = replace(active, attempt_id='not-this-physical-attempt')
        self.denied(self.ticket_selection(active), 'TICKET_EXPIRED_OR_USED', 409,
                    policy='ticket', ticket=forged_attempt)

    def test_ticket_fixed_snapshot_survives_active_change_and_retention_release(self):
        old, _lease, original = self.first()
        ticket = self.ticket(old)
        latest, _fresh, _asset = self.publish(payload=b'new active original', previous=old)
        with write_transaction(self.engine) as conn:
            conn.execute(text('UPDATE revision_seals SET retained=0 WHERE revision_id=:id'), dict(id=old))
        self.denied(self.selection(old), 'RESOURCE_NOT_FOUND', 404)
        result = self.resolve(self.ticket_selection(ticket), policy='ticket', ticket=ticket)
        self.assertEqual({a.revision_id for a in result.assets}, {old})
        self.assertEqual({a.object_sha for a in result.assets}, {original['sha256']})
        self.assertNotEqual(old, latest)
        self.admin()
        self.denied(self.selection(old), 'RESOURCE_NOT_FOUND', 404, policy='admin_retained')

    def test_ticket_pin_loss_and_current_source_revocation_deny_saved_snapshot(self):
        revision, lease, _asset = self.first()
        ticket = self.ticket(revision)
        selection = self.ticket_selection(ticket)
        with write_transaction(self.engine) as conn:
            conn.execute(text("DELETE FROM object_pins WHERE kind='ticket' AND reference_id=:id"), dict(id=ticket.ticket_id))
        self.denied(selection, 'ASSET_UNAVAILABLE', 409, policy='ticket', ticket=ticket)
        self.publisher.pin('ticket', ticket.ticket_id, [a.object_sha for a in ticket.assets])
        self.resolve(selection, policy='ticket', ticket=ticket)
        with write_transaction(self.engine) as conn:
            conn.execute(text("UPDATE publication_plans SET rights_status='blocked' WHERE job_id=:id"), dict(id=lease.job_id))
        self.denied(selection, 'ASSET_ACCESS_DENIED', 403, policy='ticket', ticket=ticket)

    def test_caller_cannot_forge_ticket_payload_or_use_snapshot_without_policy(self):
        revision, _lease, _asset = self.first()
        ticket = self.ticket(revision)
        selection = self.ticket_selection(ticket)
        changed = replace(ticket.assets[0], byte_size=ticket.assets[0].byte_size+1)
        forged = replace(ticket, assets=(changed,)+ticket.assets[1:])
        self.denied(selection, 'TICKET_SNAPSHOT_CONFLICT', 409, policy='ticket', ticket=forged)
        self.denied(selection, 'INVALID_SELECTION', 400, ticket=ticket)
        self.denied(selection, 'TICKET_AUTHORITY_REQUIRED', 403, policy='ticket')

    def test_nonrunning_or_invalidated_physical_attempt_is_not_ticket_authority(self):
        revision, _lease, _asset = self.first()
        active = self.claim(self.ticket(revision))
        with write_transaction(self.engine) as conn:
            conn.execute(text("UPDATE download_attempts SET state='cancelling' WHERE id=:id"), dict(id=active.attempt_id))
        self.denied(self.ticket_selection(active), 'TICKET_EXPIRED_OR_USED', 409, policy='ticket', ticket=active)
        other = self.claim(self.ticket(revision))
        with write_transaction(self.engine) as conn:
            conn.execute(text('UPDATE download_attempts SET invalidated=1 WHERE id=:id'), dict(id=other.attempt_id))
        self.denied(self.ticket_selection(other), 'TICKET_EXPIRED_OR_USED', 409, policy='ticket', ticket=other)
        self.assertGreater(self.scalar("SELECT count(*) FROM object_pins WHERE kind='ticket' AND reference_id=:id", id=other.ticket_id), 0)

    def test_legacy_sealed_asset_has_source_proof_but_no_preview_run(self):
        revision, lease, _asset = self.first()
        result = self.resolve(self.selection(revision))
        self.assertEqual(result.assets[0].sources[0].job_id, lease.job_id)
        self.assertEqual(result.assets[0].previews, ())
        self.assertEqual(self.scalar('SELECT count(*) FROM ingestion_runs WHERE job_id=:id', id=lease.job_id), 0)

    def ingestion_fixture(self):
        # Module composition avoids importing another TestCase into this module's
        # namespace, which would make unittest rediscover its entire test suite.
        fixture = test_ingestion.IngestionAcceptance()
        self.addCleanup(fixture.doCleanups)
        fixture.setUp()
        return fixture

    def test_nonobject_optional_preview_keeps_authorized_original_readable(self):
        fixture = self.ingestion_fixture()
        _upload, lease = fixture.queued('1화\nvalid preview positive control\n'.encode())
        revision = fixture.ingest.process_upload(lease)
        policy = LibraryReadPolicy(fixture.auth)
        with fixture.engine.connect() as conn:
            valid = policy.resolve_selection_in(conn, fixture.member.token,
                                                (AssetSelection(revision, positions=(0,)),))
            row = dict(conn.execute(text("""SELECT run.sha256 AS run_sha,run.byte_size AS run_size,
                run.preview_json,p.payload_json,i.edition_id AS input_edition,
                i.expected_revision_id AS input_previous,i.byte_size AS input_size
                FROM ingestion_runs run JOIN ingestion_inputs i ON i.job_id=run.job_id
                JOIN publication_plans p ON p.job_id=run.job_id AND p.generation=run.generation
                WHERE run.job_id=:job AND run.generation=:generation"""),
                dict(job=lease.job_id,generation=lease.generation)).mappings().one())
        asset = valid.assets[0]
        self.assertEqual(len(asset.previews), 1)
        self.assertEqual(asset.previews[0].source.job_id, lease.job_id)
        self.assertIsInstance(json.loads(row['preview_json']), dict)
        # SQL NULL and malformed JSON are defensive helper controls: the actual
        # ingestion_runs table rejects them, while valid nonobject JSON is legal.
        for unusable in (None, '{'):
            with self.subTest(defensive_json=unusable):
                self.assertIsNone(policy._preview(dict(row, preview_json=unusable),
                                                  asset.sources[0], asset.public()))
        for index, nonobject in enumerate(('null', '[]', '0', 'true', '"text"')):
            with self.subTest(persisted_json=nonobject):
                edition = 'nonobject' + str(index)
                with write_transaction(fixture.engine) as conn:
                    conn.execute(text("""INSERT INTO work_editions(id,work_id,edition_key,
                        representation,edition_label,created_at)
                        VALUES(:id,'w',:id,'text',:id,'test')"""), dict(id=edition))
                _upload, bad_lease = fixture.queued(
                    ('1화\nsource ' + str(index) + '\n').encode(), edition=edition)
                # Fault injection changes only persisted optional derivative JSON
                # at INSERT. All actual source/plan/cache checks and immutable
                # evidence triggers remain enabled; no original or seal changes.
                def stored_nonobject(conn, cursor, statement, parameters, context, many):
                    if statement.startswith('INSERT INTO ingestion_runs'):
                        self.assertFalse(many)
                        values = list(parameters)
                        values[context.compiled.positiontup.index('preview')] = nonobject
                        parameters = tuple(values)
                    return statement, parameters
                event.listen(fixture.engine, 'before_cursor_execute', stored_nonobject, retval=True)
                try:
                    bad_revision = fixture.ingest.process_upload(bad_lease)
                finally:
                    event.remove(fixture.engine, 'before_cursor_execute', stored_nonobject)
                self.assertEqual(fixture.scalar('SELECT preview_json FROM ingestion_runs WHERE job_id=:id',
                                                id=bad_lease.job_id), nonobject)
                with fixture.engine.connect() as conn:
                    resolved = policy.resolve_selection_in(conn, fixture.member.token,
                                                           (AssetSelection(bad_revision, positions=(0,)),))
                self.assertEqual(len(resolved.assets), 1)
                self.assertEqual(resolved.assets[0].sources[0].job_id, bad_lease.job_id)
                self.assertEqual(resolved.assets[0].previews, ())
                self.assertTrue(resolved.assets[0].object_sha)

    def test_real_ingested_bundle_preview_follows_each_asset_source_not_latest_seal(self):
        fixture = self.ingestion_fixture()
        _upload, old = fixture.queued('1화\nfirst body\n2화\nsecond body\n'.encode())
        previous = fixture.ingest.process_upload(old)
        _upload, new = fixture.queued('3화\nthird body\n'.encode(), previous=previous)
        revision = fixture.ingest.process_upload(new)
        option = fixture.scalar('SELECT id FROM revision_options WHERE edition_revision_id=:id', id=revision)
        policy = LibraryReadPolicy(fixture.auth)
        with fixture.engine.connect() as conn:
            result = policy.resolve_selection_in(conn, fixture.member.token, (AssetSelection(revision, option_id=option),))
        self.assertEqual(len(result.assets), 2)
        self.assertEqual([a.previews[0].source.job_id for a in result.assets], [old.job_id, new.job_id])
        self.assertEqual([a.previews[0].source.generation for a in result.assets], [old.generation, new.generation])
        self.assertEqual(fixture.scalar('SELECT source_job_id FROM revision_seals WHERE revision_id=:id', id=revision), new.job_id)
        for asset in result.assets:
            self.assertEqual(len(asset.previews), 1)
            proof = asset.previews[0]
            manifest = json.loads(proof.manifest_json)
            self.assertEqual((proof.source_sha256, proof.source_byte_size), (asset.object_sha, asset.byte_size))
            self.assertEqual((manifest['source_sha256'], manifest['source_byte_size']), (asset.object_sha, asset.byte_size))
            self.assertTrue((fixture.path / manifest['bundle_relpath']).is_file())
        self.assertNotIn('bundle_relpath', json.dumps(result.public()))
        self.assertNotIn(old.job_id, json.dumps(result.public()))

    def test_real_ingested_other_format_preview_keeps_its_original_source_binding(self):
        fixture = self.ingestion_fixture()
        _upload, text_lease = fixture.queued('1화\ntext body\n'.encode())
        previous = fixture.ingest.process_upload(text_lease)
        _upload, html_lease = fixture.queued('<h1>1화</h1><p>safe html body</p>'.encode(), filename='Book.html', previous=previous)
        revision = fixture.ingest.process_upload(html_lease)
        policy = LibraryReadPolicy(fixture.auth)
        with fixture.engine.connect() as conn:
            positions = tuple(conn.execute(text('SELECT asset_position FROM revision_assets WHERE edition_revision_id=:id ORDER BY asset_position'), dict(id=revision)).scalars())
            result = policy.resolve_selection_in(conn, fixture.member.token, (AssetSelection(revision, positions=positions),))
        by_format = {a.file_format: a for a in result.assets}
        self.assertEqual(set(by_format), {'txt', 'html'})
        self.assertEqual(by_format['txt'].previews[0].source.job_id, text_lease.job_id)
        self.assertEqual(by_format['html'].previews[0].source.job_id, html_lease.job_id)
        for asset in by_format.values():
            manifest = json.loads(asset.previews[0].manifest_json)
            self.assertEqual(manifest['source_sha256'], asset.object_sha)
            self.assertEqual(manifest['kind'], 'text')


if __name__ == '__main__':
    unittest.main()
