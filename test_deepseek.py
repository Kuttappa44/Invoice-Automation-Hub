#!/usr/bin/env python3
"""Quick test to verify DeepSeek 70B model is working"""

import os
import json
import boto3
from dotenv import load_dotenv

load_dotenv()

print("=" * 60)
print("TESTING DEEPSEEK 70B MODEL")
print("=" * 60)

# Get configuration
bedrock_model = os.getenv('AWS_BEDROCK_MODEL', 'deepseek.deepseek-r1-70b-v1:0')
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_region = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')

print(f"\nModel: {bedrock_model}")
print(f"Region: {aws_region}")

if not aws_access_key_id or not aws_secret_access_key:
    print("ERROR: AWS credentials not found in .env file")
    exit(1)

try:
    # Initialize Bedrock client
    bedrock_client = boto3.client(
        "bedrock-runtime",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=aws_region
    )
    
    print("SUCCESS: Bedrock client initialized")
    
    # Test prompt
    test_prompt = "Extract the hotel name from this invoice: Hotel Taj Palace, Bill Number: INV-12345, Guest: John Doe"
    
    print(f"\n📝 Test prompt: {test_prompt}")
    print("\n🔍 Sending request to DeepSeek 70B...")
    
    # DeepSeek chat completion format
    request_body = {
        "messages": [
            {
                "role": "user",
                "content": test_prompt
            }
        ],
        "max_tokens": 200,
        "temperature": 0.1
    }
    
    response = bedrock_client.invoke_model(
        modelId=bedrock_model,
        body=json.dumps(request_body)
    )
    
    response_body = json.loads(response['body'].read())
    
    print("\n📥 Response received:")
    print("-" * 60)
    
    # Parse DeepSeek response
    if 'choices' in response_body and len(response_body['choices']) > 0:
        result_text = response_body['choices'][0].get('message', {}).get('content', '').strip()
        print(result_text)
    elif 'output' in response_body:
        print(response_body['output'].strip())
    elif 'content' in response_body:
        print(response_body['content'].strip())
    else:
        print("Raw response:")
        print(json.dumps(response_body, indent=2))
    
    print("-" * 60)
    print("\nSUCCESS: DeepSeek 70B model is working correctly!")
    
except Exception as e:
    error_msg = str(e)
    print(f"\n❌ Error: {error_msg}")
    
    if 'ValidationException' in error_msg or 'not found' in error_msg.lower():
        print("\n💡 Possible issues:")
        print("   1. Model ID might not be available in your region")
        print("   2. Model might not be enabled in your AWS Bedrock console")
        print("   3. Try: deepseek.deepseek-r1-v1:0 (base model)")
        print("\n   To check available models, run:")
        print("   aws bedrock list-foundation-models --region us-east-1")
    elif 'AccessDeniedException' in error_msg:
        print("\n💡 AWS user doesn't have permission to invoke Bedrock models")
        print("   Add 'bedrock:InvokeModel' permission to your IAM user")
    else:
        import traceback
        traceback.print_exc()

