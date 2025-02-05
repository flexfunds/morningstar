from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import logging
from pathlib import Path
import os
from typing import Optional


class GoogleDriveService:
    def __init__(self, credentials_path: Optional[str] = None):
        """Initialize Google Drive service"""
        self.logger = logging.getLogger(__name__)

        try:
            # First try to get credentials from environment variable
            creds_json = os.getenv('GOOGLE_DRIVE_CREDENTIALS')
            if creds_json:
                credentials = service_account.Credentials.from_service_account_info(
                    eval(creds_json),
                    scopes=['https://www.googleapis.com/auth/drive']
                )
            elif credentials_path:
                credentials = service_account.Credentials.from_service_account_file(
                    credentials_path,
                    scopes=['https://www.googleapis.com/auth/drive']
                )
            else:
                raise ValueError(
                    "No credentials provided - either set GOOGLE_DRIVE_CREDENTIALS environment variable or provide credentials_path")

            self.service = build('drive', 'v3', credentials=credentials)
            self.logger.info("Google Drive service initialized successfully")
        except Exception as e:
            self.logger.error(
                f"Failed to initialize Google Drive service: {str(e)}")
            raise

    def _find_file_by_name_in_folder(self, filename: str, folder_id: str) -> Optional[str]:
        """Find a file by name in a specific folder and return its ID if found"""
        try:
            query = f"name = '{filename}' and '{
                folder_id}' in parents and trashed = false"
            results = self.service.files().list(
                q=query,
                spaces='drive',
                fields='files(id, name)',
                pageSize=1
            ).execute()
            files = results.get('files', [])

            return files[0]['id'] if files else None

        except Exception as e:
            self.logger.error(f"Failed to search for file {
                              filename}: {str(e)}")
            raise

    def upload_file(self, file_path: Path, folder_id: str) -> str:
        """Upload a file to specified Google Drive folder or update if it exists"""
        try:
            filename = file_path.name
            existing_file_id = self._find_file_by_name_in_folder(
                filename, folder_id)

            media = MediaFileUpload(str(file_path), resumable=True)

            if existing_file_id:
                # Update existing file
                file = self.service.files().update(
                    fileId=existing_file_id,
                    media_body=media,
                    fields='id'
                ).execute()
                self.logger.info(f"Successfully updated existing file {
                                 filename} in Google Drive")
            else:
                # Create new file
                file_metadata = {
                    'name': filename,
                    'parents': [folder_id]
                }
                file = self.service.files().create(
                    body=file_metadata,
                    media_body=media,
                    fields='id'
                ).execute()
                self.logger.info(f"Successfully uploaded new file {
                                 filename} to Google Drive")

            return file.get('id')

        except Exception as e:
            self.logger.error(
                f"Failed to upload/update {file_path.name}: {str(e)}")
            raise
