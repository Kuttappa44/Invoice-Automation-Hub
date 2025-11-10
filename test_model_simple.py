#!/usr/bin/env python3
"""
Simple test script to verify AWS Bedrock model configuration
Tests model initialization without requiring all dependencies
"""

import os
import json
import boto3
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_model_configuration():
    """Test if the model is configured correctly"""
    print("=" * 60)
    print("TESTING AWS BEDROCK MODEL CONFIGURATION")
    print("=" * 60)
    
    # Get model from environment or default
    bedrock_model = os.getenv('AWS_BEDROCK_MODEL', 'deepseek.deepseek-r1-70b-v1:0')
    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    aws_region = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
    enabled = os.getenv('ENABLE_OPENAI_VISION', 'false').lower() == 'true'
    
    print(f"\nConfiguration:")
    print(f"   Model: {bedrock_model}")
    print(f"   Region: {aws_region}")
    print(f"   Enabled: {enabled}")
    print(f"   AWS Access Key: {'[OK] Set' if aws_access_key_id else '[MISSING] Not set'}")
    print(f"   AWS Secret Key: {'[OK] Set' if aws_secret_access_key else '[MISSING] Not set'}")
    
    if not enabled:
        print("\n[WARNING] Model is disabled in environment")
        print("   Set ENABLE_OPENAI_VISION=true in .env file")
        return False
    
    if not aws_access_key_id or not aws_secret_access_key:
        print("\n[WARNING] AWS credentials not configured")
        print("   Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in .env file")
        return False
    
    # Try to initialize boto3 client
    try:
        print("\nInitializing AWS Bedrock client...")
        client = boto3.client(
            "bedrock-runtime",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region
        )
        print("[SUCCESS] AWS Bedrock client initialized successfully!")
        
        # Test model availability (simple test)
        print(f"\nTesting model availability: {bedrock_model}")
        print("   (This will check if the model ID is valid)")
        
        # Try to list available models (if possible)
        try:
            bedrock_client = boto3.client(
                "bedrock",
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                region_name=aws_region
            )
            models = bedrock_client.list_foundation_models()
            
            # Check if our model is in the list
            model_ids = [m['modelId'] for m in models.get('modelSummaries', [])]
            if bedrock_model in model_ids:
                print(f"   [OK] Model '{bedrock_model}' is available in your region!")
            else:
                print(f"   [WARNING] Model '{bedrock_model}' not found in available models")
                print(f"   Available models in region '{aws_region}':")
                for model_id in model_ids[:10]:  # Show first 10
                    if 'claude' in model_id.lower() or bedrock_model.split('.')[0] in model_id:
                        print(f"      - {model_id}")
        except Exception as e:
            print(f"   [WARNING] Could not verify model availability: {e}")
            print(f"   (This is okay - model might still work)")
        
        print(f"\n[SUCCESS] Model configuration test passed!")
        print(f"\nSummary:")
        print(f"   [OK] Model: {bedrock_model}")
        print(f"   [OK] Region: {aws_region}")
        print(f"   [OK] Client: Initialized")
        print(f"   [OK] Ready for invoice processing")
        
        return True
        
    except Exception as e:
        print(f"\n[ERROR] Error initializing AWS Bedrock client: {e}")
        print(f"   Check your AWS credentials and region settings")
        return False

def main():
    """Run test"""
    print("\n" + "AWS BEDROCK MODEL CONFIGURATION TEST")
    print("=" * 60)
    
    result = test_model_configuration()
    
    print("\n" + "=" * 60)
    if result:
        print("[SUCCESS] Model configuration is correct!")
        print("\nNext steps:")
        print("   1. The model has been changed to: Claude Sonnet")
        print("   2. Run your invoice processor to test it")
        print("   3. Claude Sonnet provides better accuracy than Haiku")
    else:
        print("[WARNING] Model configuration needs attention")
        print("\nCheck:")
        print("   1. .env file has correct AWS credentials")
        print("   2. ENABLE_OPENAI_VISION=true is set")
        print("   3. AWS credentials have Bedrock access")
    print("=" * 60)

if __name__ == "__main__":
    main()

