import hashlib
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import List, Dict, Optional, Tuple
import logging

logger = logging.getLogger(__name__)


class D0010Parser:
    """Parser for D0010 UFF (Uniform File Format) flow files"""
    
    # Record type identifiers
    RECORD_TYPES = {
        'ZHV': 'header',
        '026': 'mpan',
        '028': 'meter_serial',
        '030': 'reading',
        'ZPT': 'footer'
    }
    
    # Reading type mappings from D0010 specification
    READING_TYPES = {
        'A': 'actual',
        'C': 'customer',
        'D': 'deemed',
        'E': 'estimated',
        'F': 'final',
        'I': 'initial',
        'M': 'manual',
        'S': 'subsequent',
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
        Parse D0010 UFF file and return list of reading records
        Returns: (readings, file_hash)
        """
        readings = []
        file_hash = self.calculate_file_hash(filepath)
        
        current_mpan = None
        current_meter_serial = None
        
        encodings = ['utf-8', 'latin-1', 'cp1252']
        
        for encoding in encodings:
            try:
                with open(filepath, 'r', encoding=encoding) as file:
                    line_num = 0
                    
                    for line in file:
                        line_num += 1
                        line = line.strip()
                        
                        if not line:
                            continue
                        
                        # Split by pipe delimiter
                        fields = line.split('|')
                        
                        if not fields:
                            continue
                        
                        record_type = fields[0]
                        
                        if record_type == 'ZHV':
                            # Header record - validate file type
                            if len(fields) > 2 and not fields[2].startswith('D0010'):
                                self.errors.append(f"Line {line_num}: Not a D0010 file (found {fields[2]})")
                                return [], file_hash
                                
                        elif record_type == '026':
                            # MPAN record
                            current_mpan = self._parse_mpan_record(fields, line_num)
                            
                        elif record_type == '028':
                            # Meter serial number record
                            current_meter_serial = self._parse_meter_record(fields, line_num)
                            
                        elif record_type == '030':
                            # Reading record
                            if current_mpan and current_meter_serial:
                                reading = self._parse_reading_record(
                                    fields, current_mpan, current_meter_serial, line_num
                                )
                                if reading:
                                    readings.append(reading)
                            else:
                                self.warnings.append(
                                    f"Line {line_num}: Reading record without MPAN/meter"
                                )
                                
                        elif record_type == 'ZPT':
                            # Footer record - end of file
                            break
                    
                    break  # Successfully read file
                    
            except UnicodeDecodeError:
                continue
            except Exception as e:
                logger.error(f"Error reading file with {encoding} encoding: {e}")
        
        logger.info(f"Parsed {len(readings)} valid readings from {filepath}")
        return readings, file_hash
    
    def _parse_mpan_record(self, fields: List[str], line_num: int) -> Optional[str]:
        """Parse MPAN from 026 record"""
        if len(fields) < 2:
            self.warnings.append(f"Line {line_num}: Invalid MPAN record")
            return None
        
        mpan = fields[1].strip()
        return self._validate_mpan(mpan, line_num)
    
    def _parse_meter_record(self, fields: List[str], line_num: int) -> Optional[str]:
        """Parse meter serial number from 028 record"""
        if len(fields) < 2:
            self.warnings.append(f"Line {line_num}: Invalid meter record")
            return None
        
        serial = fields[1].strip()
        return self._validate_serial(serial, line_num)
    
    def _parse_reading_record(
        self, 
        fields: List[str], 
        mpan: str, 
        meter_serial: str, 
        line_num: int
    ) -> Optional[Dict]:
        """Parse reading data from 030 record"""
        # Expected format: 030|register_id|reading_datetime|reading_value|...
        if len(fields) < 4:
            self.warnings.append(f"Line {line_num}: Invalid reading record")
            return None
        
        try:
            register_id = fields[1].strip() or '01'
            
            # Parse datetime
            datetime_str = fields[2].strip()
            reading_datetime = self._parse_datetime(datetime_str, line_num)
            if not reading_datetime:
                return None
            
            # Parse reading value
            value_str = fields[3].strip()
            reading_value = self._parse_decimal(value_str, line_num)
            if reading_value is None:
                return None
            
            # Determine reading type (might be in later fields or default)
            reading_type = 'actual'  # Default
            if len(fields) > 7:
                type_indicator = fields[7].strip()
                if type_indicator in self.READING_TYPES:
                    reading_type = self.READING_TYPES[type_indicator]
            
            return {
                'mpan': mpan,
                'meter_serial': meter_serial,
                'reading_date': reading_datetime.date(),
                'reading_time': reading_datetime.time(),
                'register_id': register_id,
                'reading_value': reading_value,
                'reading_type': reading_type,
            }
            
        except Exception as e:
            self.errors.append(f"Line {line_num}: Error parsing reading - {e}")
            return None
    
    def _validate_mpan(self, mpan: str, line_num: int) -> Optional[str]:
        """Validate MPAN format (13 digits)"""
        if not mpan:
            self.warnings.append(f"Line {line_num}: Empty MPAN")
            return None
        
        # Remove any spaces
        mpan = mpan.replace(' ', '')
        
        if not mpan.isdigit() or len(mpan) != 13:
            self.warnings.append(f"Line {line_num}: Invalid MPAN format '{mpan}' (expected 13 digits)")
            return None
        
        return mpan
    
    def _validate_serial(self, serial: str, line_num: int) -> Optional[str]:
        """Validate meter serial number"""
        if not serial:
            self.warnings.append(f"Line {line_num}: Empty meter serial number")
            return None
        
        if len(serial) > 50:
            self.warnings.append(f"Line {line_num}: Meter serial too long (max 50 chars)")
            return None
        
        return serial
    
    def _parse_datetime(self, datetime_str: str, line_num: int) -> Optional[datetime]:
        """Parse datetime in YYYYMMDDHHmmss format"""
        if not datetime_str:
            self.warnings.append(f"Line {line_num}: Empty datetime")
            return None
        
        try:
            # D0010 uses YYYYMMDDHHmmss format
            # Handle cases where time might be 000000
            if len(datetime_str) == 14:
                return datetime.strptime(datetime_str, '%Y%m%d%H%M%S')
            elif len(datetime_str) == 8:
                # Date only
                return datetime.strptime(datetime_str, '%Y%m%d')
            else:
                self.warnings.append(
                    f"Line {line_num}: Invalid datetime format '{datetime_str}'"
                )
                return None
        except ValueError:
            self.warnings.append(
                f"Line {line_num}: Invalid datetime format '{datetime_str}'"
            )
            return None
    
    def _parse_decimal(self, value_str: str, line_num: int) -> Optional[Decimal]:
        """Parse decimal reading value"""
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