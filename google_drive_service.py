from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
import logging
from pathlib import Path
import os
import io
from typing import Optional, List, Dict, Any
from datetime import datetime


class GoogleDriveService:
    def __init__(self, credentials_path: Optional[str] = None):
        """Initialize Google Drive service"""
        self.logger = logging.getLogger(__name__)

        # Disable DEBUG logs from Google API Client
        logging.getLogger('googleapiclient.discovery').setLevel(
            logging.WARNING)
        logging.getLogger('google_auth_httplib2').setLevel(logging.WARNING)
        logging.getLogger('googleapiclient').setLevel(logging.WARNING)

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
        except Exception as e:
            self.logger.error(f"Drive service init failed: {str(e)}")
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
            self.logger.error(f"Search failed for {filename}")
            raise

    def list_files_in_folder(self, folder_id: str, name_contains: str = None) -> List[Dict[str, Any]]:
        """
        List all files in a folder, optionally filtered by name.
        Returns detailed file metadata including creation/modification time.
        """
        try:
            query = f"'{folder_id}' in parents and trashed = false"
            if name_contains:
                query += f" and name contains '{name_contains}'"

            results = self.service.files().list(
                q=query,
                spaces='drive',
                fields='files(id, name, createdTime, modifiedTime, mimeType)',
                orderBy='modifiedTime desc'
            ).execute()

            return results.get('files', [])
        except Exception as e:
            self.logger.error(
                f"Failed to list files in folder {folder_id}: {str(e)}")
            raise

    def get_most_recent_file(self, folder_id: str, name_contains: str = None) -> Optional[Dict[str, Any]]:
        """
        Get the most recent file in a folder, optionally filtered by name.
        Returns the file metadata for the most recently modified file.
        """
        files = self.list_files_in_folder(folder_id, name_contains)
        if not files:
            return None

        # Files are already sorted by modified time (most recent first)
        return files[0]

    def download_file(self, file_id: str, local_path: str) -> bool:
        """
        Download a file from Google Drive by ID and save it to the specified local path.
        Returns True if successful.
        """
        try:
            request = self.service.files().get_media(fileId=file_id)
            with open(local_path, 'wb') as f:
                downloader = self.service.files().get_media(fileId=file_id)
                f_handle = downloader.execute()
                f.write(f_handle)
            return True
        except Exception as e:
            self.logger.error(f"Failed to download file {file_id}: {str(e)}")
            return False

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
                # No need to log every successful file update
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
                # Only log new file uploads
                self.logger.info(f"New file uploaded: {filename}")

            return file.get('id')

        except Exception as e:
            self.logger.error(f"Upload failed: {file_path.name}")
            raise
