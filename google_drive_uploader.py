import os
import json
import io
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseUpload
from googleapiclient.errors import HttpError

# Google Drive API configuration
SCOPES = ['https://www.googleapis.com/auth/drive.file']
CREDENTIALS_FILE = 'client_secret.json'
TOKEN_FILE = 'token.json'

class GoogleDriveUploader:
    def __init__(self):
        self.service = None
        self.folder_ids = {}
        
    def authenticate(self):
        """Authenticate with Google Drive API"""
        creds = None
        
        # Check if token file exists
        if os.path.exists(TOKEN_FILE):
            try:
                creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
            except Exception as e:
                print(f"⚠️  Error loading token: {e}")
                creds = None
        
        # If no valid credentials, get new ones
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                try:
                    creds.refresh(Request())
                except Exception as e:
                    print(f"⚠️  Error refreshing token: {e}")
                    creds = None
            
            if not creds:
                if not os.path.exists(CREDENTIALS_FILE):
                    print(f"❌ Credentials file not found: {CREDENTIALS_FILE}")
                    print("📋 Please download client_secret.json from Google Cloud Console")
                    return False
                
                try:
                    flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
                    creds = flow.run_local_server(port=0)
                except Exception as e:
                    print(f"❌ Error during authentication: {e}")
                    return False
            
            # Save credentials for next run
            try:
                with open(TOKEN_FILE, 'w') as token:
                    token.write(creds.to_json())
            except Exception as e:
                print(f"⚠️  Warning: Could not save token: {e}")
        
        try:
            self.service = build('drive', 'v3', credentials=creds)
            print("✅ Google Drive authentication successful")
            return True
        except Exception as e:
            print(f"❌ Error building Drive service: {e}")
            return False
    
    def create_folder(self, folder_name, parent_folder_id=None):
        """Create a folder in Google Drive"""
        if not self.service:
            print("❌ Not authenticated with Google Drive")
            return None
        
        try:
            # Check if folder already exists
            query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder'"
            if parent_folder_id:
                query += f" and '{parent_folder_id}' in parents"
            else:
                query += " and 'root' in parents"
            
            results = self.service.files().list(
                q=query,
                fields="files(id, name)"
            ).execute()
            
            folders = results.get('files', [])
            if folders:
                folder_id = folders[0]['id']
                print(f"📁 Folder '{folder_name}' already exists (ID: {folder_id})")
                return folder_id
            
            # Create new folder
            folder_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder'
            }
            
            if parent_folder_id:
                folder_metadata['parents'] = [parent_folder_id]
            
            folder = self.service.files().create(
                body=folder_metadata,
                fields='id'
            ).execute()
            
            folder_id = folder.get('id')
            print(f"📁 Created folder '{folder_name}' (ID: {folder_id})")
            return folder_id
            
        except HttpError as error:
            print(f"❌ Error creating folder '{folder_name}': {error}")
            return None
    
    def setup_folders(self):
        """Create all required folders for invoice processing"""
        if not self.authenticate():
            return False
        
        # List of folders to create
        folder_names = ['KUMAR', 'LAKSHMI', 'MOKSHITHA', 'SANDHYA', 'UNASSIGNED']
        
        print("📁 Setting up Google Drive folders...")
        for folder_name in folder_names:
            folder_id = self.create_folder(folder_name)
            if folder_id:
                self.folder_ids[folder_name] = folder_id
            else:
                print(f"❌ Failed to create folder: {folder_name}")
                return False
        
        print("✅ All folders created successfully!")
        return True
    
    def upload_file(self, file_path, folder_name):
        """Upload a file to the specified Google Drive folder - ONLY PDFs"""
        if not self.service:
            if not self.authenticate():
                return False
        
        if folder_name not in self.folder_ids:
            print(f"❌ Folder '{folder_name}' not found in folder list")
            return False
        
        if not os.path.exists(file_path):
            print(f"❌ File not found: {file_path}")
            return False
        
        # Check if file is a PDF
        file_name = os.path.basename(file_path)
        if not file_name.lower().endswith('.pdf'):
            print(f"⚠️  Skipping non-PDF file: {file_name} (only PDFs are uploaded to Drive)")
            return False
        
        try:
            # Get file info
            file_size = os.path.getsize(file_path)
            
            print(f"📤 Uploading PDF: {file_name} ({file_size} bytes) to {folder_name}")
            
            # Create file metadata
            file_metadata = {
                'name': file_name,
                'parents': [self.folder_ids[folder_name]]
            }
            
            # Upload file
            media = MediaFileUpload(file_path, mimetype='application/pdf', resumable=True)
            file = self.service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id'
            ).execute()
            
            file_id = file.get('id')
            print(f"✅ PDF uploaded successfully! File ID: {file_id}")
            return True
            
        except HttpError as error:
            print(f"❌ Error uploading PDF: {error}")
            return False
        except Exception as error:
            print(f"❌ Unexpected error: {error}")
            return False
    
    def upload_multiple_files(self, file_paths, folder_name):
        """Upload multiple files to the same folder"""
        success_count = 0
        total_count = len(file_paths)
        
        print(f"📤 Uploading {total_count} files to {folder_name}...")
        
        for file_path in file_paths:
            if self.upload_file(file_path, folder_name):
                success_count += 1
        
        print(f"✅ Uploaded {success_count}/{total_count} files successfully")
        return success_count == total_count
    
    def upload_pdf_data(self, pdf_data, filename, folder_name):
        """Upload PDF data directly to Google Drive folder"""
        if not self.service:
            print("❌ Google Drive service not initialized")
            return None
        
        if folder_name not in self.folder_ids:
            print(f"❌ Folder '{folder_name}' not found")
            return None
        
        try:
            print(f"📤 Uploading PDF: {filename} ({len(pdf_data)} bytes) to {folder_name}")
            
            # Create file metadata
            file_metadata = {
                'name': filename,
                'parents': [self.folder_ids[folder_name]]
            }
            
            # Create media upload from bytes
            media = MediaIoBaseUpload(io.BytesIO(pdf_data), mimetype='application/pdf', resumable=True)
            
            # Upload file
            file = self.service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id'
            ).execute()
            
            file_id = file.get('id')
            print(f"✅ PDF uploaded successfully! File ID: {file_id}")
            return file_id
            
        except HttpError as error:
            print(f"❌ Error uploading PDF: {error}")
            return None
        except Exception as error:
            print(f"❌ Unexpected error uploading PDF: {error}")
            return None

    def update_file(self, file_path, file_id):
        """Update an existing file in Google Drive"""
        if not self.service:
            print("❌ Google Drive service not initialized")
            return None
        
        try:
            print(f"📤 Updating file: {os.path.basename(file_path)} ({os.path.getsize(file_path)} bytes)")
            
            # Create media upload
            media = MediaFileUpload(file_path, resumable=True)
            
            # Update file
            file = self.service.files().update(
                fileId=file_id,
                media_body=media,
                fields='id'
            ).execute()
            
            updated_file_id = file.get('id')
            print(f"✅ Updated successfully! File ID: {updated_file_id}")
            return updated_file_id
            
        except HttpError as error:
            print(f"❌ Error updating file: {error}")
            return None
        except Exception as error:
            print(f"❌ Unexpected error updating file: {error}")
            return None


def test_google_drive_setup():
    """Test function to verify Google Drive setup"""
    uploader = GoogleDriveUploader()
    
    if uploader.setup_folders():
        print("\n🎉 Google Drive setup completed successfully!")
        print("📋 Folder IDs:")
        for folder_name, folder_id in uploader.folder_ids.items():
            print(f"   {folder_name}: {folder_id}")
        return True
    else:
        print("\n❌ Google Drive setup failed!")
        return False

if __name__ == "__main__":
    test_google_drive_setup()
