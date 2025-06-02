from django.test import TestCase
from django.core.exceptions import ValidationError
from decimal import Decimal
from datetime import date, time
from .models import FlowFile, MeterPoint, Meter, Reading


class MeterPointModelTest(TestCase):
    def test_create_meter_point(self):
        """Test creating a meter point with valid MPAN"""
        mp = MeterPoint.objects.create(mpan='1234567890123')
        self.assertEqual(mp.mpan, '1234567890123')
        self.assertEqual(str(mp), 'MPAN: 1234567890123')
    
    def test_invalid_mpan_length(self):
        """Test MPAN validation for length"""
        mp = MeterPoint(mpan='12345')  # Too short
        with self.assertRaises(ValidationError):
            mp.full_clean()
    
    def test_invalid_mpan_format(self):
        """Test MPAN validation for non-numeric"""
        mp = MeterPoint(mpan='123456789012A')  # Contains letter
        with self.assertRaises(ValidationError):
            mp.full_clean()
    
    def test_mpan_uniqueness(self):
        """Test MPAN must be unique"""
        MeterPoint.objects.create(mpan='1234567890123')
        with self.assertRaises(Exception):
            MeterPoint.objects.create(mpan='1234567890123')


class MeterModelTest(TestCase):
    def setUp(self):
        self.meter_point = MeterPoint.objects.create(mpan='1234567890123')
    
    def test_create_meter(self):
        """Test creating a meter"""
        meter = Meter.objects.create(
            serial_number='ABC123',
            meter_point=self.meter_point,
            meter_type='smart'
        )
        self.assertEqual(meter.serial_number, 'ABC123')
        self.assertEqual(meter.meter_point, self.meter_point)
        self.assertEqual(str(meter), 'Meter: ABC123 (MPAN: 1234567890123)')
    
    def test_serial_number_uniqueness(self):
        """Test meter serial number must be unique"""
        Meter.objects.create(
            serial_number='ABC123',
            meter_point=self.meter_point
        )
        mp2 = MeterPoint.objects.create(mpan='9876543210987')
        with self.assertRaises(Exception):
            Meter.objects.create(
                serial_number='ABC123',
                meter_point=mp2
            )


class ReadingModelTest(TestCase):
    def setUp(self):
        self.flow_file = FlowFile.objects.create(
            filename='test.txt',
            file_hash='testhash123'
        )
        self.meter_point = MeterPoint.objects.create(mpan='1234567890123')
        self.meter = Meter.objects.create(
            serial_number='ABC123',
            meter_point=self.meter_point
        )
    
    def test_create_reading(self):
        """Test creating a reading"""
        reading = Reading.objects.create(
            meter=self.meter,
            flow_file=self.flow_file,
            reading_date=date(2024, 1, 15),
            reading_time=time(14, 30),
            reading_value=Decimal('12345.67'),
            reading_type='actual'
        )
        self.assertEqual(reading.reading_value, Decimal('12345.67'))
        self.assertEqual(
            str(reading),
            'ABC123 - 2024-01-15: 12345.67'
        )
    
    def test_reading_uniqueness(self):
        """Test reading uniqueness constraint"""
        Reading.objects.create(
            meter=self.meter,
            flow_file=self.flow_file,
            reading_date=date(2024, 1, 15),
            reading_time=time(14, 30),
            register_id='01',
            reading_value=Decimal('12345.67')
        )
        
        # Same meter, date, time, and register should fail
        with self.assertRaises(Exception):
            Reading.objects.create(
                meter=self.meter,
                flow_file=self.flow_file,
                reading_date=date(2024, 1, 15),
                reading_time=time(14, 30),
                register_id='01',
                reading_value=Decimal('99999.99')
            )


class FlowFileModelTest(TestCase):
    def test_create_flow_file(self):
        """Test creating a flow file"""
        flow_file = FlowFile.objects.create(
            filename='D0010_20240115.txt',
            file_hash='abc123def456'
        )
        self.assertEqual(flow_file.status, 'pending')
        self.assertIn('D0010_20240115.txt', str(flow_file))
    
    def test_file_hash_uniqueness(self):
        """Test file hash must be unique"""
        FlowFile.objects.create(
            filename='file1.txt',
            file_hash='samehash'
        )
        with self.assertRaises(Exception):
            FlowFile.objects.create(
                filename='file2.txt',
                file_hash='samehash'
            )