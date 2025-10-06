import os
import base64
import io
from openai import OpenAI
from pdf2image import convert_from_bytes
from PIL import Image
import PyPDF2

class OpenAIPropertyExtractor:
    def __init__(self):
        self.client = None
        self.api_key = os.getenv('OPENAI_API_KEY')
        self.enabled = os.getenv('ENABLE_OPENAI_VISION', 'false').lower() == 'true'
        
        if self.enabled and self.api_key and self.api_key != 'your_openai_api_key_here':
            try:
                self.client = OpenAI(api_key=self.api_key)
                print("✅ OpenAI Vision API initialized successfully")
            except Exception as e:
                print(f"⚠️  OpenAI initialization failed: {e}")
                self.enabled = False
        else:
            print("⚠️  OpenAI Vision disabled - API key not configured")
            self.enabled = False
    
    def pdf_to_images(self, pdf_data, max_pages=3):
        """Convert PDF to images for vision analysis"""
        try:
            # Convert PDF to images with higher DPI for better quality
            images = convert_from_bytes(
                pdf_data, 
                first_page=1, 
                last_page=max_pages, 
                dpi=300,  # Higher DPI for better quality
                fmt='PNG',
                thread_count=1
            )
            return images
        except Exception as e:
            print(f"⚠️  Error converting PDF to images: {e}")
            return []
    
    def image_to_base64(self, image):
        """Convert PIL image to base64 for OpenAI API"""
        try:
            # Convert to RGB if needed
            if image.mode != 'RGB':
                image = image.convert('RGB')
            
            # Resize image if too large (OpenAI has size limits)
            max_size = 2048  # Increased for better quality
            if image.width > max_size or image.height > max_size:
                # Maintain aspect ratio
                ratio = min(max_size/image.width, max_size/image.height)
                new_width = int(image.width * ratio)
                new_height = int(image.height * ratio)
                image = image.resize((new_width, new_height), Image.Resampling.LANCZOS)
            
            # Convert to base64
            buffer = io.BytesIO()
            image.save(buffer, format='PNG', optimize=True)
            img_base64 = base64.b64encode(buffer.getvalue()).decode()
            return img_base64
        except Exception as e:
            print(f"⚠️  Error converting image to base64: {e}")
            return None
    
    def extract_comprehensive_invoice_data_from_image(self, image):
        """Use OpenAI Vision to extract comprehensive invoice data from image"""
        if not self.enabled or not self.client:
            return None
        
        try:
            # Convert image to base64
            img_base64 = self.image_to_base64(image)
            if not img_base64:
                return None
            
            # Create comprehensive vision prompt
            prompt = """
            Analyze this hotel invoice PDF image and extract the important details. Return the information in a clear, readable format with these details:

            HOTEL: [Hotel name from logo or header]
            GUEST: [Guest name with proper title]
            BILL NO: [Invoice/Bill number]
            BILL DATE: [Invoice date]
            ROOM: [Room number]
            GUESTS: [Number of guests]
            CHECK-IN: [Check-in date]
            CHECK-OUT: [Check-out date]
            AMOUNT: [Total amount]
            GST: [GST number if visible]

            Instructions:
            - Look at the entire image including logos, headers, and all text
            - Extract the actual hotel/property name, not company names
            - Use proper date formats (DD/MM/YYYY or DD-MM-YYYY)
            - Include currency symbol for amounts (₹ or Rs.)
            - Look for guest names in any format (Mr./Ms./Dr. etc.)
            - Find bill numbers, room numbers, and amounts anywhere in the document
            - If any information is not clearly visible or readable, write "Not Found"
            - Be thorough and check all sections of the document
            - Return only the formatted information, no explanations or additional text
            """
            
            # Call OpenAI Vision API
            response = self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": prompt
                            },
                            {
                                "type": "image_url",
                                "image_url": {
                                    "url": f"data:image/png;base64,{img_base64}"
                                }
                            }
                        ]
                    }
                ],
                max_tokens=800,
                temperature=0.1
            )
            
            result_text = response.choices[0].message.content.strip()
            
            # Return the formatted text directly (no JSON parsing)
            print(f"   ✅ Successfully extracted invoice data")
            return result_text
            
        except Exception as e:
            print(f"⚠️  OpenAI Vision API error: {e}")
            return None
    
    def extract_property_name_from_image(self, image):
        """Use OpenAI Vision to extract property name from image (legacy method)"""
        comprehensive_data = self.extract_comprehensive_invoice_data_from_image(image)
        if comprehensive_data and comprehensive_data.get('hotel_name'):
            return comprehensive_data['hotel_name']
        return None
    
    def extract_comprehensive_invoice_data_from_pdf(self, pdf_data):
        """Extract comprehensive invoice data from PDF using OpenAI Vision"""
        if not self.enabled:
            return None
        
        try:
            # Convert PDF to images
            images = self.pdf_to_images(pdf_data, max_pages=2)  # First 2 pages only
            
            if not images:
                return None
            
            # Try each page until we find comprehensive data
            for i, image in enumerate(images):
                print(f"   🔍 Analyzing page {i+1} with OpenAI Vision...")
                invoice_data = self.extract_comprehensive_invoice_data_from_image(image)
                
                if invoice_data and len(invoice_data.strip()) > 50:  # Check if we got substantial data
                    # Check if it contains useful information (not just "Not Found" everywhere)
                    useful_info = any(keyword in invoice_data for keyword in [
                        "HOTEL:", "GUEST:", "BILL", "ROOM:", "CHECK-IN", "CHECK-OUT", "AMOUNT:", "GST:"
                    ])
                    
                    if useful_info:
                        print(f"   ✅ Found comprehensive invoice data on page {i+1}")
                        return invoice_data
                    else:
                        print(f"   ⚠️  Data found but not useful on page {i+1}")
                        print(f"   📝 Sample: {invoice_data[:100]}...")
                else:
                    print(f"   ⚠️  No comprehensive data found on page {i+1}")
                    if invoice_data:
                        print(f"   📝 Raw response: {invoice_data[:200]}...")
            
            return None
            
        except Exception as e:
            print(f"⚠️  Error in OpenAI comprehensive extraction: {e}")
            return None
    
    def extract_property_name_from_pdf(self, pdf_data):
        """Extract property name from PDF using OpenAI Vision (legacy method)"""
        comprehensive_data = self.extract_comprehensive_invoice_data_from_pdf(pdf_data)
        if comprehensive_data and "HOTEL:" in comprehensive_data:
            # Extract hotel name from the formatted text
            lines = comprehensive_data.split('\n')
            for line in lines:
                if line.startswith('HOTEL:'):
                    hotel_name = line.replace('HOTEL:', '').strip()
                    if hotel_name and hotel_name != "Not Found":
                        return hotel_name
        return None
    
