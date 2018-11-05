Help

---***---
glacier_media_backup.py

Requires: glacier_dynamo_config.py
Requires: AWS cli with read/write to glacier and dynamo

Script that will parse a folder for folders that contain media to upload to Glacier for long term backup storage.  If a file has not already been loaded, it will upload and then save the Archive Information in a DynamoDB.

