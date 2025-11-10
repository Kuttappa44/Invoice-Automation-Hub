#!/usr/bin/env python3
"""
Test script to verify AWS Bedrock model is working correctly
Tests the invoice extraction with the configured model
"""

import os
from dotenv import load_dotenv
from openai_vision_extractor import OpenAIPropertyExtractor

# Load environment variables
load_dotenv()

def test_model_initialization():
    """Test if the model initializes correctly"""
    print("=" * 60)
    print("🧪 TESTING AWS BEDROCK MODEL INITIALIZATION")
    print("=" * 60)
    
    extractor = OpenAIPropertyExtractor()
    
    if not extractor.enabled:
        print("\n❌ Model is not enabled!")
        print("   Make sure you have set in .env file:")
        print("   - ENABLE_OPENAI_VISION=true")
        print("   - AWS_ACCESS_KEY_ID=your_key")
        print("   - AWS_SECRET_ACCESS_KEY=your_secret")
        print("   - AWS_DEFAULT_REGION=us-east-1")
        return False
    
    print(f"\n✅ Model initialized successfully!")
    print(f"   Model ID: {extractor.bedrock_model}")
    print(f"   Region: {extractor.aws_region}")
    print(f"   Client: {'Initialized' if extractor.client else 'Not initialized'}")
    
    return True

def test_model_with_sample_pdf():
    """Test model with a sample PDF if available"""
    print("\n" + "=" * 60)
    print("🧪 TESTING MODEL WITH PDF EXTRACTION")
    print("=" * 60)
    
    extractor = OpenAIPropertyExtractor()
    
    if not extractor.enabled:
        print("❌ Model not enabled. Skipping PDF test.")
        return False
    
    # Check if there's a sample PDF in the directory
    sample_pdfs = []
    if os.path.exists('sample_invoice.pdf'):
        sample_pdfs.append('sample_invoice.pdf')
    if os.path.exists('test_invoice.pdf'):
        sample_pdfs.append('test_invoice.pdf')
    
    if not sample_pdfs:
        print("⚠️  No sample PDF found in directory.")
        print("   To test PDF extraction, place a sample invoice PDF named:")
        print("   - sample_invoice.pdf")
        print("   - test_invoice.pdf")
        return False
    
    # Test with first available PDF
    pdf_path = sample_pdfs[0]
    print(f"\n📄 Testing with: {pdf_path}")
    
    try:
        with open(pdf_path, 'rb') as f:
            pdf_data = f.read()
        
        print(f"   PDF size: {len(pdf_data)} bytes")
        print(f"   Extracting invoice data...")
        
        # Extract comprehensive data
        result = extractor.extract_comprehensive_invoice_data_from_pdf(pdf_data)
        
        if result:
            print("\n✅ Extraction successful!")
            print("\n📋 Extracted Data:")
            print("-" * 60)
            print(result)
            print("-" * 60)
            return True
        else:
            print("\n❌ Extraction failed - no data returned")
            return False
            
    except FileNotFoundError:
        print(f"❌ File not found: {pdf_path}")
        return False
    except Exception as e:
        print(f"❌ Error during extraction: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_model_api_format():
    """Test if the API format is correct for the model"""
    print("\n" + "=" * 60)
    print("🧪 TESTING API FORMAT COMPATIBILITY")
    print("=" * 60)
    
    extractor = OpenAIPropertyExtractor()
    
    if not extractor.enabled:
        print("❌ Model not enabled. Skipping API format test.")
        return False
    
    model_id = extractor.bedrock_model
    print(f"\n📋 Model ID: {model_id}")
    
    # Check model provider
    if 'claude' in model_id.lower() or 'anthropic' in model_id.lower():
        print("✅ Model is Claude (Anthropic) - uses vision API format")
        print("   ✅ API format: Anthropic Claude message format")
        print("   ✅ Vision support: Yes")
        return True
    elif 'openai' in model_id.lower() or 'gpt' in model_id.lower():
        print("⚠️  Model is OpenAI - text-only, no vision support")
        print("   ⚠️  Current code uses vision API format")
        print("   ⚠️  May need code modifications for OpenAI models")
        return False
    else:
        print("⚠️  Unknown model type")
        return False

def main():
    """Run all tests"""
    print("\n" + "🚀 AWS BEDROCK MODEL TEST SUITE")
    print("=" * 60)
    
    results = []
    
    # Test 1: Model Initialization
    results.append(("Model Initialization", test_model_initialization()))
    
    # Test 2: API Format Check
    results.append(("API Format Compatibility", test_model_api_format()))
    
    # Test 3: PDF Extraction (if sample available)
    results.append(("PDF Extraction", test_model_with_sample_pdf()))
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 TEST SUMMARY")
    print("=" * 60)
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"   {status} - {test_name}")
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    print(f"\n   Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 All tests passed! Model is working correctly.")
    else:
        print("\n⚠️  Some tests failed. Check the output above for details.")

if __name__ == "__main__":
    main()

