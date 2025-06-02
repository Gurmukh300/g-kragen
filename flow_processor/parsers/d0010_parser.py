import csv
import hashlib
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import List, Dict, Optional, Tuple
import logging

logger = logging.getLogger(__name__)


class D0010Parser:
    """Parser for D0010 flow files"""
    
    # D0010 field positions (0-indexed)
    FIELD_POSITIONS = {
        'record_type': 0,
        'mpan': 1,
        'meter_serial': 2,
        'reading_date': 3,
        'reading_time': 4,
        'register_id': 5,
        'reading_value': 6,
        'reading_type': 7,
    }
    
    # Valid reading types in D0010
    READING_TYPES = {
        'A': 'actual',
        'E': 'estimated',
        'C': 'customer',
    }
    
    def __init__(self):
        self.errors = []
        self.warnings = []
        
    def calculate_file_hash(self, filepath: str) -> str:
        """Calculate SHA256 hash of file for duplicate detection"""
        sha256_hash = hashlib.sha256()
        with open(filepath, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    
    def parse_file(self, filepath: str) -> Tuple[List[Dict], str]:
        """
        Parse D0010 file and return list of reading records
        Returns: (readings, file_hash)
        """
        readings = []
        file_hash = self.calculate_file_hash(filepath)
        
        encodings = ['utf-8', 'latin-1', 'cp1252']
        
        for encoding in encodings:
            try:
                with open(filepath, 'r', encoding=encoding) as file:
                    reader = csv.reader(file, delimiter='|')
                    
                    for line_num, row in enumerate(reader, 1):
                        try:
                            reading = self._parse_row(row, line_num)
                            if reading:
                                readings.append(reading)
                        except Exception as e:
                            self.errors.append(f"Line {line_num}: {str(e)}")
                            logger.error(f"Error parsing line {line_num}: {e}")
                    
                    break  # Successfully read file
                    
            except UnicodeDecodeError:
                continue
            except Exception as e:
                logger.error(f"Error reading file with {encoding} encoding: {e}")
        
        logger.info(f"Parsed {len(readings)} valid readings from {filepath}")
        return readings, file_hash
    
    def _parse_row(self, row: List[str], line_num: int) -> Optional[Dict]:
        """Parse a single row from D0010 file"""
        # Skip empty rows
        if not row or all(field.strip() == '' for field in row):
            return None
        
        # Check minimum field count
        if len(row) < 8:
            self.warnings.append(f"Line {line_num}: Insufficient fields (expected 8, got {len(row)})")
            return None
        
        try:
            # Extract and validate fields
            mpan = self._validate_mpan(row[self.FIELD_POSITIONS['mpan']], line_num)
            if not mpan:
                return None
            
            meter_serial = self._validate_serial(row[self.FIELD_POSITIONS['meter_serial']], line_num)
            if not meter_serial:
                return None
            
            reading_date = self._parse_date(row[self.FIELD_POSITIONS['reading_date']], line_num)
            if not reading_date:
                return None
            
            reading_time = self._parse_time(row[self.FIELD_POSITIONS['reading_time']], line_num)
            
            register_id = row[self.FIELD_POSITIONS['register_id']].strip() or '01'
            
            reading_value = self._parse_decimal(row[self.FIELD_POSITIONS['reading_value']], line_num)
            if reading_value is None:
                return None
            
            reading_type_code = row[self.FIELD_POSITIONS['reading_type']].strip().upper()
            reading_type = self.READING_TYPES.get(reading_type_code, 'actual')
            
            return {
                'mpan': mpan,
                'meter_serial': meter_serial,
                'reading_date': reading_date,
                'reading_time': reading_time,
                'register_id': register_id,
                'reading_value': reading_value,
                'reading_type': reading_type,
            }
            
        except IndexError as e:
            self.errors.append(f"Line {line_num}: Field access error - {e}")
            return None
    
    def _validate_mpan(self, mpan: str, line_num: int) -> Optional[str]:
        """Validate MPAN format (13 digits)"""
        mpan = mpan.strip()
        if not mpan:
            self.warnings.append(f"Line {line_num}: Empty MPAN")
            return None
        
        if not mpan.isdigit() or len(mpan) != 13:
            self.warnings.append(f"Line {line_num}: Invalid MPAN format '{mpan}' (expected 13 digits)")
            return None
        
        return mpan
    
    def _validate_serial(self, serial: str, line_num: int) -> Optional[str]:
        """Validate meter serial number"""
        serial = serial.strip()
        if not serial:
            self.warnings.append(f"Line {line_num}: Empty meter serial number")
            return None
        
        if len(serial) > 50:
            self.warnings.append(f"Line {line_num}: Meter serial too long (max 50 chars)")
            return None
        
        return serial
    
    def _parse_date(self, date_str: str, line_num: int) -> Optional[datetime.date]:
        """Parse date in YYYYMMDD format"""
        date_str = date_str.strip()
        if not date_str:
            self.warnings.append(f"Line {line_num}: Empty date")
            return None
        
        try:
            # D0010 uses YYYYMMDD format
            return datetime.strptime(date_str, '%Y%m%d').date()
        except ValueError:
            self.warnings.append(f"Line {line_num}: Invalid date format '{date_str}' (expected YYYYMMDD)")
            return None
    
    def _parse_time(self, time_str: str, line_num: int) -> Optional[datetime.time]:
        """Parse time in HHMM format"""
        time_str = time_str.strip()
        if not time_str:
            return None
        
        try:
            # D0010 uses HHMM format
            return datetime.strptime(time_str, '%H%M').time()
        except ValueError:
            self.warnings.append(f"Line {line_num}: Invalid time format '{time_str}' (expected HHMM)")
            return None
    
    def _parse_decimal(self, value_str: str, line_num: int) -> Optional[Decimal]:
        """Parse decimal reading value"""
        value_str = value_str.strip()
        if not value_str:
            self.warnings.append(f"Line {line_num}: Empty reading value")
            return None
        
        try:
            value = Decimal(value_str)
            if value < 0:
                self.warnings.append(f"Line {line_num}: Negative reading value {value}")
                return None
            return value
        except InvalidOperation:
            self.warnings.append(f"Line {line_num}: Invalid decimal value '{value_str}'")
            return None