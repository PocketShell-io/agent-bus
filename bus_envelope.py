def validate_bus_envelope(envelope: dict) -> tuple[bool, str]:
    """
    Validates that an Agent Bus envelope dictionary has all required fields.
    Required fields: 'message_id', 'sender_id', 'recipient_id', 'body', 'created_at', 'idempotency_key'.
    Returns (True, 'ok') if valid.
    Returns (False, 'missing: <field>') if any field is missing or empty.
    """
    required_string_fields = [
        'message_id',
        'sender_id',
        'recipient_id',
        'created_at',
        'idempotency_key'
    ]

    for field in required_string_fields:
        if field not in envelope:
            return False, f'missing: {field}'
        val = envelope[field]
        if not isinstance(val, str) or not val:
            return False, f'missing: {field}'

    if 'body' not in envelope or envelope['body'] is None:
        return False, 'missing: body'

    return True, 'ok'

if __name__ == '__main__':
    import unittest

    class TestValidateBusEnvelope(unittest.TestCase):
        def setUp(self):
            self.valid_envelope = {
                'message_id': 'msg-123',
                'sender_id': 'agent-a',
                'recipient_id': 'agent-b',
                'body': '{"task": "do something"}',
                'created_at': '2026-10-06T01:35:00Z',
                'idempotency_key': 'idem-123'
            }

        def test_valid_envelope(self):
            is_valid, msg = validate_bus_envelope(self.valid_envelope)
            self.assertTrue(is_valid)
            self.assertEqual(msg, 'ok')

        def test_missing_field(self):
            for field in self.valid_envelope.keys():
                invalid_envelope = self.valid_envelope.copy()
                del invalid_envelope[field]
                
                is_valid, msg = validate_bus_envelope(invalid_envelope)
                self.assertFalse(is_valid)
                self.assertEqual(msg, f'missing: {field}')

        def test_empty_field(self):
            for field in self.valid_envelope.keys():
                if field == 'body':
                    continue
                invalid_envelope = self.valid_envelope.copy()
                invalid_envelope[field] = ''
                
                is_valid, msg = validate_bus_envelope(invalid_envelope)
                self.assertFalse(is_valid)
                self.assertEqual(msg, f'missing: {field}')

        def test_none_field(self):
            for field in self.valid_envelope.keys():
                invalid_envelope = self.valid_envelope.copy()
                invalid_envelope[field] = None
                
                is_valid, msg = validate_bus_envelope(invalid_envelope)
                self.assertFalse(is_valid)
                self.assertEqual(msg, f'missing: {field}')

        def test_falsy_body(self):
            falsy_bodies = [{}, "", False, 0, []]
            for body in falsy_bodies:
                valid_falsy = self.valid_envelope.copy()
                valid_falsy['body'] = body
                is_valid, msg = validate_bus_envelope(valid_falsy)
                self.assertTrue(is_valid)
                self.assertEqual(msg, 'ok')

    unittest.main()
