import sys
import os
import boto3
import time
import glacier_dynamo_config


# Function to archive a file to Glacier and log the archive information to DynamoDB
def archive_file(file, archive_type, filename, title, dyn_table):
    # Create a Glacier Client
    gclient = boto3.client('glacier')

    # Open the file in bit mode
    with open(file, 'rb') as f:
        # Upload the archive
        # Description is the title of the Morvie
        return_val = gclient.upload_archive(vaultName = glacier_dynamo_config.GLACIER_VAULT_NAME, archiveDescription=title, body=f)

    # Get the ArchiveID that was assigned to the upload
    #  so that it can be saved in the DynamoDB
    archiveID = return_val['archiveId']

    # Add an entry to the Dynamo DB for this Archive
    dyn_table.put_item(
    Item={
            'Title': title,
            'Filename': os.path.basename(filename),
            'File': file,
            'ArchiveType': archive_type,
            'ArchiveID': archiveID,
        }
    )

    # Done!
    return True


# Takes the folder provided and checks to see if it includes and media files to archive
def process_media_files(media_folder, dyn_table, global_counter):
    # Loop through the files in the folder
    for filename in os.listdir(media_folder):
        # Must be a file, and end in either .mp4 or .m4v
        if os.path.isfile(os.path.join(media_folder, filename)) and (filename.endswith('.mp4') or filename.endswith('m4v')):
            # Create a Movie Objects
            # Includes Title, File (full path), Filename
            my_movie = {"Title": filename.replace('_', ' ').replace('.mp4','').replace('.m4v',''),
                        "File": os.path.join(media_folder, filename),
                        "Filename": filename}
            # Check to see if it already exists in the Dynamo DB Table
            # The key is the title of the movie
            response = dyn_table.get_item(
                Key={
                    'Title': filename.replace('_', ' ').replace('.mp4','').replace('.m4v',''),
                }
            )
            # Was it already there?
            if "Item" in response.keys():
                # It exists, just update the Movie object
            	my_movie.update({"Exists": True})
            else:
                # It does not exist, state so then archive
            	my_movie.update({"Exists": False})

                # Start the Glacier archive process
                archive_file(my_movie['File'], 'Movie', my_movie['Filename'], my_movie['Title'], dyn_table)

            # Print the Movie information for Debug purposes
            print(my_movie)

# DynamoDB client and Table Object
dynamodb = boto3.resource('dynamodb')
archive_table = dynamodb.Table(glacier_dynamo_config.DYNAMO_TABLE_NAME)

# Get the base folder where we'll start searching from the arguments
base_folder = str(sys.argv[1])

# Make sure they gave us a folder to start in
if not(os.path.isdir(base_folder)):
    quit()

# Get all Folders
for my_folder in os.listdir(base_folder):
    # Confirm this is a sub-folder
    if os.path.isdir(os.path.join(base_folder, my_folder)):
        # Run all the files in the folder
        process_media_files(os.path.join(base_folder, my_folder), archive_table)
        # Sleep for 1 second to maintain read capacity
        time.sleep(1)
