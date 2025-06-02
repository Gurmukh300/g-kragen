import os
import tempfile
from io import StringIO
from django.test import TestCase
from django.core.management import call_command
from django.core.management.base import CommandError
from .models import FlowFile, MeterPoint, Meter, Reading


class ImportD0010CommandTest(TestCase):
    def setUp(self):
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
    
    def test_import_valid_file(self):
        """Test importing a valid D0010 file"""
        content = """ZHV|0000475656|D0010002|D|UDMS|X|MRCY|20160302153151||||OPER|
026|1234567890123|V|
028|ABC123|D|
030|01|20240115143000|12345.67|||T|A|
026|9876543210987|V|
028|XYZ789|C|
030|01|20240116093000|54321.00|||T|E|
ZPT|0000475656|2||2|20160302154650|"""
        
        filepath = self.create_test_file(content)
        
        out = StringIO()
        call_command('import_d0010', filepath, stdout=out)
        
        # Check database
        self.assertEqual(FlowFile.objects.count(), 1)
        self.assertEqual(MeterPoint.objects.count(), 2)
        self.assertEqual(Meter.objects.count(), 2)
        self.assertEqual(Reading.objects.count(), 2)
        
        # Check output
        output = out.getvalue()
        self.assertIn('Successfully imported 2 readings', output)
    
    def test_import_nonexistent_file(self):
        """Test error handling for nonexistent file"""
        out = StringIO()
        
        # The command handles errors gracefully and doesn't raise CommandError to the caller
        call_command('import_d0010', '/nonexistent/file.txt', stdout=out)
        
        output = out.getvalue()
        # Check that the error was handled and reported
        self.assertIn('Failed to process /nonexistent/file.txt', output)
        self.assertIn('File not found', output)
        self.assertIn('0 succeeded, 1 failed', output)
    
    def test_import_duplicate_file(self):
        """Test preventing duplicate imports"""
        content = """ZHV|0000475656|D0010002|D|UDMS|X|MRCY|20160302153151||||OPER|
026|1234567890123|V|
028|ABC123|D|
030|01|20240115143000|12345.67|||T|A|
ZPT|0000475656|1||1|20160302154650|"""
        
        filepath = self.create_test_file(content)
        
        # First import
        call_command('import_d0010', filepath)
        
        # Second import should be prevented
        out = StringIO()
        call_command('import_d0010', filepath, stdout=out)
        
        output = out.getvalue()
        self.assertIn('already imported', output)
        self.assertEqual(Reading.objects.count(), 1)  # No new readings
    
    def test_force_reimport(self):
        """Test force flag allows reimport"""
        content = """ZHV|0000475656|D0010002|D|UDMS|X|MRCY|20160302153151||||OPER|
026|1234567890123|V|
028|ABC123|D|
030|01|20240115143000|12345.67|||T|A|
ZPT|0000475656|1||1|20160302154650|"""
        
        filepath = self.create_test_file(content)
        
        # First import
        call_command('import_d0010', filepath)
        
        # Force reimport
        call_command('import_d0010', filepath, '--force')
        
        # Should update existing reading, not create duplicate
        self.assertEqual(Reading.objects.count(), 1)
    
    def test_dry_run(self):
        """Test dry run doesn't save to database"""
        content = """ZHV|0000475656|D0010002|D|UDMS|X|MRCY|20160302153151||||OPER|
026|1234567890123|V|
028|ABC123|D|
030|01|20240115143000|12345.67|||T|A|
ZPT|0000475656|1||1|20160302154650|"""
        
        filepath = self.create_test_file(content)
        
        out = StringIO()
        call_command('import_d0010', filepath, '--dry-run', stdout=out)
        
        # Nothing should be saved
        self.assertEqual(FlowFile.objects.count(), 0)
        self.assertEqual(Reading.objects.count(), 0)
        
        # But should show what would be imported
        output = out.getvalue()
        self.assertIn('Would import 1 readings', output)
    
    def test_multiple_files(self):
        """Test importing multiple files"""
        content1 = """ZHV|0000475656|D0010002|D|UDMS|X|MRCY|20160302153151||||OPER|
026|1234567890123|V|
028|ABC123|D|
030|01|20240115143000|12345.67|||T|A|
ZPT|0000475656|1||1|20160302154650|"""
        
        content2 = """ZHV|0000475657|D0010002|D|UDMS|X|MRCY|20160302153151||||OPER|
026|9876543210987|V|
028|XYZ789|C|
030|01|20240116093000|54321.00|||T|E|
ZPT|0000475657|1||1|20160302154650|"""
        
        filepath1 = self.create_test_file(content1, 'file1.txt')
        filepath2 = self.create_test_file(content2, 'file2.txt')
        
        call_command('import_d0010', filepath1, filepath2)
        
        self.assertEqual(FlowFile.objects.count(), 2)
        self.assertEqual(Reading.objects.count(), 2)
    
    def test_meter_movement(self):
        """Test handling meter moving between MPANs"""
        # First file - meter ABC123 at MPAN 1234567890123
        content1 = """ZHV|0000475656|D0010002|D|UDMS|X|MRCY|20160302153151||||OPER|
026|1234567890123|V|
028|ABC123|D|
030|01|20240115143000|12345.67|||T|A|
ZPT|0000475656|1||1|20160302154650|"""
        
        # Second file - same meter at different MPAN
        content2 = """ZHV|0000475657|D0010002|D|UDMS|X|MRCY|20160302153151||||OPER|
026|9876543210987|V|
028|ABC123|D|
030|01|20240116143000|23456.78|||T|A|
ZPT|0000475657|1||1|20160302154650|"""
        
        filepath1 = self.create_test_file(content1, 'file1.txt')
        filepath2 = self.create_test_file(content2, 'file2.txt')
        
        call_command('import_d0010', filepath1)
        call_command('import_d0010', filepath2)
        
        # Should have 2 MPANs but only 1 meter
        self.assertEqual(MeterPoint.objects.count(), 2)
        self.assertEqual(Meter.objects.count(), 1)
        
        # Meter should be associated with the newer MPAN
        meter = Meter.objects.get(serial_number='ABC123')
        self.assertEqual(meter.meter_point.mpan, '9876543210987')
    
    def test_import_no_readings(self):
        """Test handling file with no readings"""
        content = """ZHV|0000475656|D0010002|D|UDMS|X|MRCY|20160302153151||||OPER|
ZPT|0000475656|0||0|20160302154650|"""
        
        filepath = self.create_test_file(content)
        
        out = StringIO()
        call_command('import_d0010', filepath, stdout=out)
        
        output = out.getvalue()
        self.assertIn('No valid readings found', output)
        self.assertEqual(Reading.objects.count(), 0)