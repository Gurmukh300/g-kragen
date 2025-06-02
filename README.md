# G-Kraken Flow File Processor D0010

**Author:** Gurmukh Singh  

## Overview

G-Kraken is a Django web application designed to process D0010 energy industry flow files. These pipe-delimited text files contain meter reading information that needs to be:

1. Imported into a database
2. Made available for browsing by support staff through a web interface

## Features

### Core Functionality
- Command-line import of D0010 flow files
- Database storage of:
  - Meter readings
  - Meter points
  - Meter information

### Web Interface
- Django admin interface with search capabilities for:
  - MPAN (Meter Point Administration Number)
  - Meter serial number
- Display of:
  - Reading values
  - Reading dates
  - Source filename

### Quality Assurance
- Comprehensive test suite
- Robust error handling and validation

## Requirements

### Software
- **Python:** 3.10 or higher
- **Django:** 4.2+
- **Database:** SQLite
- **OS:** macOS or Linux

## Installation Guide

### 1. Set up virtual environment

python3 -m venv venv
source venv/bin/activate

pip install -r requirements.txt

# Create migrations
python manage.py makemigrations flow_processor

# Apply migrations
python manage.py migrate

# Run tests
python manage.py test

# Import a single file
python manage.py import_d0010 DTC5259515123502080915D0010.uff

# OR using sample file
python manage.py import_d0010 sample_d0010.txt

# Create admin user
python manage.py createsuperuser

# Run server
python manage.py runserver
