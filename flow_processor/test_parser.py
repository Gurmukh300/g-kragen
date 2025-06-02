import os
import tempfile
from django.test import TestCase
from datetime import date, time
from decimal import Decimal
from .parsers.d0010_parser import D0010Parser


class D0010ParserTest(TestCase):
    def setUp(self):
        self.parser = D0010Parser()
        self.temp_dir = tempfile.mkdtemp()
    
    def tearDown(self):
        # Clean up temp files
        for file in os.listdir(self.temp_dir):
            os.remove(os.path.join(self.temp_dir, file))
        os.rmdir(self.temp_dir)
    
    def create_test_file(self, content, filename='test_d0010.txt'):
        """Helper to create test file"""
        filepath = os.path.join(self.temp_dir, filename)
        with open(filepath, 'w') as f:
            f.write(content)
        return filepath
    
    def test_parse_valid_file(self):
        """Test parsing a valid D0010 UFF file"""
        content = """ZHV|0000475656|D0010002|D|UDMS|X|MRCY|20160302153151||||OPER|
026|1234567890123|V|
028|ABC123|D|
030|01|20240115143000|12345.67|||T|A|
026|9876543210987|V|
028|XYZ789|C|
030|01|20240116093000|54321.00|||T|E|
ZPT|0000475656|2||2|20160302154650|"""
        
        filepath = self.create_test_file(content)
        readings, file_hash = self.parser.parse_file(filepath)
        
        self.assertEqual(len(readings), 2)
        
        # Check first reading
        self.assertEqual(readings[0]['mpan'], '1234567890123')
        self.assertEqual(readings[0]['meter_serial'], 'ABC123')
        self.assertEqual(readings[0]['reading_date'], date(2024, 1, 15))
        self.assertEqual(readings[0]['reading_time'], time(14, 30))
        self.assertEqual(readings[0]['reading_value'], Decimal('12345.67'))
        self.assertEqual(readings[0]['reading_type'], 'actual')
        
        # Check second reading
        self.assertEqual(readings[1]['mpan'], '9876543210987')
        self.assertEqual(readings[1]['reading_type'], 'estimated')  # Default when not specified
    
    def test_skip_empty_lines(self):
        """Test parser skips empty lines"""
        content = """ZHV|0000475656|D0010002|D|UDMS|X|MRCY|20160302153151||||OPER|
026|1234567890123|V|
028|ABC123|D|
030|01|20240115143000|12345.67|||T|A|

026|9876543210987|V|
028|XYZ789|C|
030|01|20240116093000|54321.00|||T|E|

ZPT|0000475656|2||2|20160302154650|"""
        
        filepath = self.create_test_file(content)
        readings, _ = self.parser.parse_file(filepath)
        
        self.assertEqual(len(readings), 2)
    
    def test_invalid_mpan(self):
        """Test handling of invalid MPAN"""
        content = """ZHV|0000475656|D0010002|D|UDMS|X|MRCY|20160302153151||||OPER|
026|123|V|
028|ABC123|D|
030|01|20240115143000|12345.67|||T|A|
026|123456789012X|V|
028|ABC124|D|
030|01|20240115143000|12345.67|||T|A|
ZPT|0000475656|2||2|20160302154650|"""
        
        filepath = self.create_test_file(content)
        readings, _ = self.parser.parse_file(filepath)
        
        self.assertEqual(len(readings), 0)  # Both should be rejected
        self.assertEqual(len(self.parser.warnings), 4)
    
    def test_invalid_date_format(self):
        """Test handling of invalid date"""
        content = """ZHV|0000475656|D0010002|D|UDMS|X|MRCY|20160302153151||||OPER|
026|1234567890123|V|
028|ABC123|D|
030|01|15/01/2024|12345.67|||T|A|
ZPT|0000475656|1||1|20160302154650|"""
        
        filepath = self.create_test_file(content)
        readings, _ = self.parser.parse_file(filepath)
        
        self.assertEqual(len(readings), 0)
        self.assertTrue(any('Invalid datetime format' in w for w in self.parser.warnings))
    
    def test_missing_fields(self):
        """Test handling of missing fields"""
        content = """ZHV|0000475656|D0010002|D|UDMS|X|MRCY|20160302153151||||OPER|
026|1234567890123|V|
028|ABC123|D|
030|01|
ZPT|0000475656|1||1|20160302154650|"""
        
        filepath = self.create_test_file(content)
        readings, _ = self.parser.parse_file(filepath)
        
        self.assertEqual(len(readings), 0)
        self.assertTrue(any('Invalid reading record' in w for w in self.parser.warnings))
    
    def test_file_hash_consistency(self):
        """Test file hash is consistent"""
        content = """ZHV|0000475656|D0010002|D|UDMS|X|MRCY|20160302153151||||OPER|
026|1234567890123|V|
028|ABC123|D|
030|01|20240115143000|12345.67|||T|A|
ZPT|0000475656|1||1|20160302154650|"""
        
        filepath = self.create_test_file(content)
        _, hash1 = self.parser.parse_file(filepath)
        _, hash2 = self.parser.parse_file(filepath)
        
        self.assertEqual(hash1, hash2)
    
    def test_reading_type_mapping(self):
        """Test reading type code mapping"""
        content = """ZHV|0000475656|D0010002|D|UDMS|X|MRCY|20160302153151||||OPER|
026|1234567890123|V|
028|ABC123|D|
030|01|20240115143000|12345.67|||T|A|
030|01|20240116143000|12345.67|||T|C|
030|01|20240117143000|12345.67|||T|D|
030|01|20240118143000|12345.67|||T|X|
ZPT|0000475656|4||4|20160302154650|"""
        
        filepath = self.create_test_file(content)
        readings, _ = self.parser.parse_file(filepath)
        
        self.assertEqual(len(readings), 4)
        self.assertEqual(readings[0]['reading_type'], 'actual')
        self.assertEqual(readings[1]['reading_type'], 'customer')
        self.assertEqual(readings[2]['reading_type'], 'deemed')
        self.assertEqual(readings[3]['reading_type'], 'actual')  # Unknown defaults to actual
    
    def test_parse_valid_file_complete(self):
        """Test parsing a complete D0010 file with multiple readings"""
        content = """ZHV|0000475656|D0010002|D|UDMS|X|MRCY|20160302153151||||OPER|
026|1234567890123|V|
028|ABC123|D|
030|01|20240115000000|12345.67|||T|A|
030|02|20240115000000|54321.00|||T|A|
026|9876543210987|V|
028|XYZ789|C|
030|01|20240116000000|98765.43|||T|E|
ZPT|0000475656|3||3|20160302154650|"""
        
        filepath = self.create_test_file(content)
        readings, _ = self.parser.parse_file(filepath)
        
        self.assertEqual(len(readings), 3)
        
        # First meter has two readings
        meter1_readings = [r for r in readings if r['meter_serial'] == 'ABC123']
        self.assertEqual(len(meter1_readings), 2)
        self.assertEqual(meter1_readings[0]['register_id'], '01')
        self.assertEqual(meter1_readings[1]['register_id'], '02')