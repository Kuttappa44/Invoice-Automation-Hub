#!/usr/bin/env python3
"""
OpenAI Property Extractor using AWS Bedrock
Extracts structured data from PDFs, images, and text using AWS Bedrock models
"""

import os
import json
import base64
import re
import boto3
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class OpenAIPropertyExtractor:
    """Extract structured data using AWS Bedrock models"""
    
    def __init__(self):
        """Initialize the extractor with AWS Bedrock configuration"""
        self.enabled = os.getenv('ENABLE_OPENAI_VISION', 'false').lower() == 'true'
        self.bedrock_model = os.getenv('AWS_BEDROCK_MODEL', 'amazon.nova-pro-v1:0')
        self.aws_region = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
        self.aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
        self.aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        
        self.client = None
        
        if self.enabled:
            try:
                self.client = boto3.client(
                    "bedrock-runtime",
                    aws_access_key_id=self.aws_access_key_id,
                    aws_secret_access_key=self.aws_secret_access_key,
                    region_name=self.aws_region
                )
                print(f"   ✅ AWS Bedrock client initialized (Model: {self.bedrock_model})")
            except Exception as e:
                print(f"   ⚠️  Failed to initialize AWS Bedrock client: {e}")
                self.enabled = False
                self.client = None
    
    def _invoke_bedrock_text(self, prompt):
        """Invoke AWS Bedrock with text-only input - simplified approach"""
        if not self.enabled or not self.client:
            return None
        
        try:
            model_id = self.bedrock_model.lower()
            
            # For Amazon Nova models - use Messages API format
            # Nova requires content to be a JSONArray, not a string
            if 'nova' in model_id or 'amazon' in model_id:
                body = {
                    "messages": [
                        {
                            "role": "user",
                            "content": [
                                {
                                    "text": prompt
                                }
                            ]
                        }
                    ],
                    "inferenceConfig": {
                        "maxTokens": 8192,
                        "temperature": 0.1
                    }
                }
            # For Claude/Anthropic models
            elif 'claude' in model_id or 'anthropic' in model_id:
                body = {
                    "anthropic_version": "bedrock-2023-05-31",
                    "max_tokens": 8192,
                    "temperature": 0.1,
                    "messages": [
                        {
                            "role": "user",
                            "content": prompt
                        }
                    ]
                }
            # For other models - try simple prompt format
            else:
                body = {
                    "prompt": prompt,
                    "max_tokens": 8192,
                    "temperature": 0.1
                }
            
            body_json = json.dumps(body)
            
            # Invoke the model
            response = self.client.invoke_model(
                modelId=self.bedrock_model,
                body=body_json,
                contentType="application/json",
                accept="application/json"
            )
            
            # Parse response
            response_body = json.loads(response['body'].read())
            
            # Extract text based on response format
            if 'nova' in model_id or 'amazon' in model_id:
                # Nova format: {"output": {"message": {"content": [{"text": "..."}]}}}
                try:
                    return response_body['output']['message']['content'][0]['text']
                except (KeyError, IndexError, TypeError):
                    # Try alternative paths
                    return response_body.get('output', {}).get('message', {}).get('content', '')
            elif 'claude' in model_id or 'anthropic' in model_id:
                return response_body.get('content', [{}])[0].get('text', '')
            else:
                # Try common response formats
                return (response_body.get('generation') or 
                       response_body.get('outputs', [{}])[0].get('text', '') or
                       response_body.get('results', [{}])[0].get('outputText', '') or
                       str(response_body))
                
        except Exception as e:
            print(f"   ⚠️  Error invoking AWS Bedrock: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def extract_comprehensive_invoice_data_from_pdf(self, pdf_data):
        """Extract comprehensive invoice data from PDF using AWS Bedrock
        Extracts text from PDF first, then uses Bedrock to analyze and extract structured data"""
        if not self.enabled or not self.client:
            return None
        
        try:
            # Extract text from PDF using PyPDF2
            import PyPDF2
            import io
            
            pdf_reader = PyPDF2.PdfReader(io.BytesIO(pdf_data))
            pdf_text = ""
            for page in pdf_reader.pages:
                pdf_text += page.extract_text() + "\n"
            
            if not pdf_text or len(pdf_text.strip()) < 50:
                print(f"   ⚠️  Could not extract sufficient text from PDF")
                return None
            
            # Limit text size to avoid token limits (keep first 20000 chars)
            text_snippet = pdf_text[:20000] if len(pdf_text) > 20000 else pdf_text
            
            prompt = f"""You are an expert invoice data extraction system. Analyze the following invoice PDF text and extract structured information.

Extract the following fields from the invoice:

HOTEL: <hotel name or property name>
GUEST: <guest name or customer name>
BILL NO: <bill number or invoice number>
BILL DATE: <bill date or invoice date>
CHECK-IN: <check-in date or arrival date>
CHECK-OUT: <check-out date or departure date>
ROOM: <room number>
GUESTS: <number of guests or pax>
AMOUNT: <total amount or grand total>
GST: <GST number if available>
PAN: <PAN number if available>

If a field is not found, leave it empty. Extract dates in their original format.

INVOICE TEXT:
{text_snippet}

OUTPUT FORMAT:
HOTEL: <value or empty>
GUEST: <value or empty>
BILL NO: <value or empty>
BILL DATE: <value or empty>
CHECK-IN: <value or empty>
CHECK-OUT: <value or empty>
ROOM: <value or empty>
GUESTS: <value or empty>
AMOUNT: <value or empty>
GST: <value or empty>
PAN: <value or empty>"""

            print(f"   🤖 Using AWS Bedrock ({self.bedrock_model}) to extract invoice data from PDF...")
            result = self._invoke_bedrock_text(prompt)
            
            if result:
                print(f"   ✅ AWS Bedrock extracted invoice data")
                return result
            else:
                print(f"   ⚠️  AWS Bedrock extraction returned no data")
                return None
                
        except Exception as e:
            print(f"   ❌ Error extracting invoice data from PDF: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def extract_property_name_from_pdf(self, pdf_data):
        """Extract property/hotel name from PDF using AWS Bedrock"""
        if not self.enabled or not self.client:
            return None
        
        try:
            # Extract text from PDF using PyPDF2
            import PyPDF2
            import io
            
            pdf_reader = PyPDF2.PdfReader(io.BytesIO(pdf_data))
            pdf_text = ""
            for page in pdf_reader.pages:
                pdf_text += page.extract_text() + "\n"
            
            if not pdf_text or len(pdf_text.strip()) < 50:
                return None
            
            # Limit text size (keep first 10000 chars for hotel name extraction)
            text_snippet = pdf_text[:10000] if len(pdf_text) > 10000 else pdf_text
            
            prompt = f"""You are an expert at extracting hotel/property names from invoices. 

Analyze the following invoice text and extract ONLY the hotel name or property name. 
Look for words like "Hotel", "Resort", "Inn", "Lodge", "Palace", etc. followed by the property name.

Return ONLY the hotel/property name, nothing else. If not found, return "Not Found".

INVOICE TEXT:
{text_snippet}

HOTEL NAME:"""

            print(f"   🤖 Using AWS Bedrock ({self.bedrock_model}) to extract hotel name...")
            result = self._invoke_bedrock_text(prompt)
            
            if result:
                # Clean up the response - extract just the hotel name
                hotel_name = result.strip()
                # Remove any labels like "HOTEL NAME:" or "Hotel:" from the response
                hotel_name = re.sub(r'^(?:HOTEL\s*NAME|HOTEL|PROPERTY|PROPERTY\s*NAME)[:\s]*', '', hotel_name, flags=re.IGNORECASE)
                hotel_name = hotel_name.strip()
                
                if hotel_name and hotel_name.lower() not in ['not found', 'none', 'n/a', '']:
                    print(f"   ✅ Extracted hotel name: {hotel_name}")
                    return hotel_name
            
            return None
                
        except Exception as e:
            print(f"   ⚠️  Error extracting hotel name from PDF: {e}")
            return None
    
    def extract_comprehensive_invoice_data_from_jpeg(self, jpeg_data):
        """Extract comprehensive invoice data from JPEG using AWS Bedrock"""
        # For JPEG images, we would need Bedrock's vision capabilities
        # For now, return None - JPEG processing requires vision API
        return None
    
    def extract_booking_details_from_email(self, email_body, email_html=None, email_subject=None):
        """Extract booking details from email body/text using AWS Bedrock
        Focuses on extracting from HTML tables where booking data is typically structured"""
        if not self.enabled or not self.client:
            return None
        
        # Use HTML if available, otherwise use plain text
        # Prefer HTML as it contains more structured data (tables are usually in HTML)
        text_to_analyze = email_html if email_html else email_body
        
        if not text_to_analyze:
            return None
        
        # Combine subject and body for analysis (booking codes are often in subject)
        if email_subject:
            combined_text = f"EMAIL SUBJECT: {email_subject}\n\nEMAIL BODY:\n{text_to_analyze}"
            print(f"   📧 Including email subject in analysis")
        else:
            combined_text = text_to_analyze
        
        # Use the FULL email content (up to 30000 chars to capture entire email)
        # This ensures we analyze the complete email including all tables and sections
        # Take your time - analyze thoroughly
        text_snippet = combined_text[:30000] if len(combined_text) > 30000 else combined_text
        
        print(f"   📧 Email content length: {len(text_snippet)} characters")
        
        prompt = f"""You are an expert data extraction system. Your task is to carefully analyze the ENTIRE email (including subject line) and extract ONLY actual data values.

⚠️ CRITICAL RULE - READ THIS FIRST:
DO NOT EXTRACT FIELD LABELS OR HEADERS AS VALUES!
If you see "Booking Code: 12345", extract ONLY "12345", NOT "Booking Code"
If you see a table header row like "Guest Name | Check-In Date | Check-Out Date", 
DO NOT extract "Guest Name Check-In Date Check-Out Date" - that's the HEADER, not data!

EXAMPLES OF WHAT NOT TO EXTRACT:
❌ WRONG: "Booking Code" (this is a label)
❌ WRONG: "Guest Name Check-In Date Check-Out Date" (this is a header row - DO NOT EXTRACT THIS!)
❌ WRONG: "Guest Name Check-In Date Check-Out Date Paid on Advance" (this is a header row - DO NOT EXTRACT!)
❌ WRONG: "Check-In Date" (this is a field label)
❌ WRONG: Any text that contains multiple field names together (that's a header row)

✅ CORRECT: "123456" (actual booking code number)
✅ CORRECT: "John Smith" (actual guest name)
✅ CORRECT: "15/05/2025" (actual date value)
✅ CORRECT: "1234567" (actual booking ID from a data cell)

VERIFICATION CHECKLIST - Before extracting ANY value, ask yourself:
1. Does this contain words like "Booking Code", "Guest Name", "Check-In Date" as the VALUE?
   → If YES, this is a LABEL/HEADER - SKIP IT!
2. Does this look like multiple field names together?
   → If YES, this is a HEADER ROW - SKIP IT!
3. Is this a number, date, or person's name?
   → If YES, this is likely DATA - EXTRACT IT!

STEP-BY-STEP INSTRUCTIONS:
1. Read the ENTIRE email from start to finish - take your time, analyze every section
2. **PRIORITY: Look for HTML tables in the email** - most booking data is in tables
3. For tables (MOST IMPORTANT):
   a. Identify the table structure:
      - First row = HEADER ROW (contains column names like "Booking Code", "Guest Name", etc.)
      - Second row onwards = DATA ROWS (contains actual booking information)
   b. Map column positions:
      - Find which column contains "Booking Code" (or "Booking ID", "Ref", etc.)
      - Find which column contains "Guest Name" (or "Client Name", "Name", etc.)
      - Find which column contains "Check-In Date" (or "Check In", "Arrival", etc.)
      - Find which column contains "Check-Out Date" (or "Check Out", "Departure", etc.)
   c. Extract data from DATA ROWS ONLY:
      - For each data row (row 2, 3, 4, etc.):
        * Read the value in the "Booking Code" column → Extract that value
        * Read the value in the "Guest Name" column → Extract that value
        * Read the value in the "Check-In Date" column → Extract that value
        * Read the value in the "Check-Out Date" column → Extract that value
      - Example: If row 2 has "| 123456 | Amarnath Mandal | 21/09/2025 | 23/09/2025 |"
        → Extract: Booking Code="123456", Guest Name="Amarnath Mandal", Check-In="21/09/2025", Check-Out="23/09/2025"
   d. **CRITICAL**: Skip the header row completely - it contains labels, not data!
4. If no table found, look for labeled fields:
   - Look for the field label (e.g., "Booking Code:", "Guest Name:", etc.)
   - Then look NEXT TO or BELOW that label for the actual VALUE
   - Extract ONLY the value, never the label
5. If you see multiple data rows in the table, extract each row as a separate booking
6. Be patient - analyze the entire email systematically, but focus on tables first

EMAIL CONTENT TO ANALYZE (includes subject line if available):
{text_snippet}

IMPORTANT: The email subject line may contain the Booking Code/ID. Check the SUBJECT line first!

EXTRACT THE FOLLOWING FIELDS (search for ALL variations):

1. BOOKING CODE / BOOKING ID (HIGHEST PRIORITY - MUST FIND THIS):
   - Search EVERYWHERE in the email for: "Booking Code", "Booking ID", "Booking Reference", 
     "Confirmation Number", "Booking Number", "Booking No", "Reservation ID", "Confirmation Code", 
     "Booking", "Ref No", "Reference", "Confirmation", "Reservation Code"
   - Extract the actual numeric/alphanumeric code/ID value
   - Look in tables, headers, body text, signatures - EVERYWHERE
   - This is the MOST IMPORTANT field - make sure you find it!

2. GUEST NAME / CLIENT NAME:
   - Search for: "Guest Name", "Name", "Guest", "Customer Name", "Client Name", "Client", 
     "Customer", "Guest Name:", "Name:", "Client Name:", "Client Name", "Client:"
   - Extract the full name of the guest/client
   - Look for this field in tables, email body, and all sections

3. CHECK-IN DATE:
   - Search for: "Check-In Date", "Check In Date", "Check In", "Arrival Date", "Arrival", 
     "Check-in Date", "Checkin Date", "From Date", "Start Date"
   - Extract the actual date value in any format (DD/MM/YYYY, MM-DD-YYYY, DD-MM-YYYY, etc.)

4. CHECK-OUT DATE:
   - Search for: "Check-Out Date", "Check Out Date", "Check Out", "Departure Date", "Departure", 
     "Check-out Date", "Checkout Date", "To Date", "End Date"
   - Extract the actual date value in any format

SEARCH STRATEGY (PRIORITY ORDER):
1. **FIRST**: Look for HTML tables in the email body
   - Tables are the PRIMARY source of booking data
   - Identify header row (row 1) - skip it
   - Extract data from data rows (row 2 onwards)
   - Match column names to find the right data cells
2. **SECOND**: Look in plain text sections for labeled fields
3. **THIRD**: Look in email subject line (for Booking Code)
4. **FOURTH**: Look in email signatures
5. **LAST**: Look in forwarded email content
- Ignore field labels - extract only the actual data values
- If a value appears next to a label, extract the value, not the label
- **REMEMBER**: Tables have headers in row 1, data in rows 2+ - extract from rows 2+ only!

OUTPUT FORMAT (strictly follow this format):
BOOKING CODE: <extracted value or leave empty if not found>
GUEST NAME: <extracted value or leave empty if not found>
CLIENT NAME: <extracted value if different from Guest Name, or leave empty>
CHECK-IN DATE: <extracted value or leave empty if not found>
CHECK-OUT DATE: <extracted value or leave empty if not found>

If multiple bookings are found in the email, extract each separately:
BOOKING 1:
BOOKING CODE: <value>
GUEST NAME: <value>
CHECK-IN DATE: <value>
CHECK-OUT DATE: <value>

BOOKING 2:
BOOKING CODE: <value>
GUEST NAME: <value>
CHECK-IN DATE: <value>
CHECK-OUT DATE: <value>

🔴 STRICT EXTRACTION RULES - FOLLOW THESE EXACTLY:

1. VALUE vs LABEL - The MOST IMPORTANT RULE:
   - If you see: "Booking Code: 123456" → Extract ONLY "123456"
   - If you see: "Guest Name: John Smith" → Extract ONLY "John Smith"
   - If you see: "Check-In Date: 15/05/2025" → Extract ONLY "15/05/2025"
   - NEVER extract the part before the colon (that's the label)
   - NEVER extract multiple labels together (e.g., "Guest Name Check-In Date Check-Out Date")

2. TABLE EXTRACTION (CRITICAL - Most emails have data in tables):
   - Tables have HEADER ROWS (row 1) and DATA ROWS (rows 2, 3, 4, etc.)
   - Header row typically contains column names like: "Booking Code | Guest Name | Check-In Date | Check-Out Date | Paid on | Advance"
   - Header row is ONLY for identification - DO NOT extract from header row!
   - Data rows contain actual values like: "123456 | John Smith | 15/05/2025 | 20/05/2025 | 01/01/2025 | 5000"
   - Extract ONLY from DATA ROWS (rows 2 onwards)
   - In each data row:
     * Find the "Booking Code" column - extract the VALUE from that column's cell (e.g., "123456")
     * Find the "Guest Name" column - extract the VALUE from that column's cell (e.g., "John Smith")
     * Find the "Check-In Date" column - extract the VALUE from that column's cell (e.g., "21/09/2025")
     * Find the "Check-Out Date" column - extract the VALUE from that column's cell (e.g., "23/09/2025")
   - The table might look like this:
     HEADER: | Booking Code | Guest Name | Check-In Date | Check-Out Date | Paid on | Advance |
     DATA:   | 123456       | John Doe   | 21/09/2025   | 23/09/2025    | 01/01/25| 5000    |
   - Extract "123456" (Booking Code), "John Doe" (Guest Name), "21/09/2025" (Check-In), "23/09/2025" (Check-Out)
   - DO NOT extract "Booking Code", "Guest Name", etc. - those are column headers!

3. VERIFICATION:
   - Before extracting a value, ask: "Is this a label/header or actual data?"
   - If it's a label (like "Booking Code", "Guest Name", "Check-In Date"), SKIP IT
   - If it's actual data (like "123456", "John Smith", "15/05/2025"), EXTRACT IT

4. DATE FORMATS:
   - Extract dates exactly as they appear: "15/05/2025", "15-05-2025", "2025-05-15", etc.
   - Do NOT change the date format

5. MULTIPLE BOOKINGS:
   - If there are multiple bookings, extract each one separately
   - Each booking should have its own set of values

6. THOROUGH ANALYSIS:
   - Go through the ENTIRE email word by word if needed
   - Take your time - better to be slow and accurate than fast and wrong
   - Read every section: headers, body, tables, signatures, forwarded content

7. BOOKING CODE EXTRACTION - HIGHEST PRIORITY:
   - **IN TABLES**: Look in the first data column or the column labeled "Booking Code", "Booking ID", "Ref", "Reference", etc.
   - The Booking Code is typically in the FIRST COLUMN of data rows in the table
   - Search for ANY number, code, or identifier that could be a booking reference
   - Look for patterns like: "123456", "ABC123", "2025-1234", "BK-123456", etc.
   - If you see numbers near words like "Booking", "Reference", "Confirmation", "ID", "Code", extract them
   - Even if it's just labeled as "Ref:", "Ref No:", "ID:", extract the number
   - Check every table cell in data rows - booking codes are often in the first column
   - Look in email subject lines - booking codes are sometimes there
   - If you find ANY booking-related number in a table data row, that's the Booking Code!
   - **REMEMBER**: In the table format shown in screenshots, Booking Code is usually the FIRST value in each data row

Now carefully analyze the email below. Remember: 
1. Extract VALUES, not LABELS!
2. BOOKING CODE is the MOST IMPORTANT field - find it!
"""

        print(f"   🤖 Using AWS Bedrock ({self.bedrock_model}) to extract booking details...")
        print(f"   📧 Analyzing email content ({len(text_snippet)} characters)...")
        
        try:
            result = self._invoke_bedrock_text(prompt)
            
            if result:
                print(f"   ✅ AI extraction completed ({len(result)} characters)")
                print(f"   📋 Raw AI Response preview (first 500 chars):")
                print(f"   {result[:500]}")
                return result
            else:
                print(f"   ⚠️  AI extraction returned no data")
                return None
                
        except Exception as e:
            print(f"   ❌ Error during AI extraction: {e}")
            import traceback
            traceback.print_exc()
            return None
