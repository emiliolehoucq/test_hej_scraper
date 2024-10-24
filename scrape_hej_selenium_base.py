# Script to scrape Counselor Education jobs from HigherEdJobs
# Emilio Lehoucq

# TODO: figure out how long a job can run on GitHub Actions and whether that'll be a problem

##################################### Importing libraries #####################################
import logging
from seleniumbase import Driver
from selenium.webdriver.common.by import By
from time import sleep
from random import uniform
import numpy as np
from datetime import datetime
from text_extractor import extract_text
import os
from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaFileUpload
import json

##################################### Setting parameters #####################################

# URL to scrape
URL_COUNSELOR_EDUCATION_JOBS = "https://www.higheredjobs.com/faculty/search.cfm?JobCat=62"

# Sleep time
MIN_SLEEP_TIME = 2
MAX_SLEEP_TIME = 6

# Number of retries
RETRIES = 5

# Current timestamp
TS = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

##################################### Configure the logging settings #####################################
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.info(f"Logging configured. Current timestamp: {TS}")

# # TODO: comment for GitHub Actions
# # Add a FileHandler to log to a file in the current working directory
# file_handler = logging.FileHandler('scrape_hej.log')
# file_handler.setLevel(logging.INFO)
# formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
# file_handler.setFormatter(formatter)
# logger.addHandler(file_handler)

##################################### Define functions for this script #####################################

def scroll_down_slowly():
    """
    Function to scroll down slowly to the end of the page.

    Parameters: None
    Returns: None
    """

    try:
        logger.info("Will try to scroll to the end of the page.")

        # Scroll slowly to the end of the page
        while True:
            # Scroll down
            driver.execute_script("window.scrollBy(0, 500);")
            # Sleep some time
            sleep(uniform(MIN_SLEEP_TIME, MAX_SLEEP_TIME))
            # Check if we are at the end of the page
            if driver.execute_script("return (window.innerHeight + window.scrollY) >= document.body.scrollHeight"):
                # If we are at the end of the page, break the loop
                break
        logger.info("Scrolled to the end of the page.")

        # Sleep some time
        sleep(uniform(MIN_SLEEP_TIME, MAX_SLEEP_TIME))
        logger.info("Slept for a bit.")

    except Exception as e:
        logger.info(f"Couldn't scroll to the end of the page. Error: {e}")

    return None

def extract_job_code(url):
    """
    Function to extract the job code from a URL.

    Input: url (str) - The URL containing the job code.
    Output: job_code (str) - The extracted job code.
    """
    # Find the starting index of 'JobCode=' and '&Title'
    start_index = url.find("JobCode=") + len("JobCode=")
    end_index = url.find("&Title")
    
    # Extract the job code by slicing the string
    job_code = url[start_index:end_index]
    
    return job_code

def upload_file(element_id, file_suffix, content, folder_id, service, logger):
    """
    Function to upload a file to Google Drive.

    Inputs:
    - element_id: ID of the job post
    - file_suffix: suffix of the file name
    - content: content of the file
    - folder_id: ID of the folder in Google Drive
    - service: service for Google Drive
    - logger: logger

    Outputs: None

    Dependencies: from googleapiclient.http import MediaFileUpload, os
    """
    
    logger.info(f"Inside upload_file: uploading ID {element_id} to Google Drive.")

    try:
        # Prepare the file name
        file_name = f"{element_id}_{file_suffix}.txt"
        logger.info(f"Inside upload_file: prepared the name of the file for the {file_suffix}")

        # Write the content to a temporary file
        with open(file_name, 'w') as temp_file:
            temp_file.write(content)
        logger.info(f"Inside upload_file: wrote the {file_suffix} string to a temporary file")

        # Prepare the file metadata
        file_metadata = {
            'name': file_name,
            'parents': [folder_id]
        }
        logger.info(f"Inside upload_file: prepared the file metadata for the {file_suffix}")

        # Prepare the file media
        media = MediaFileUpload(file_name, mimetype='text/plain')
        logger.info(f"Inside upload_file: prepared the file media for the {file_suffix}")

        # Upload the file to the Drive folder
        service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        logger.info(f"Inside upload_file: uploaded the file to the shared folder for the {file_suffix}")

        # Remove the temporary file after uploading
        os.remove(file_name)
        logger.info(f"Inside upload_file: removed the temporary file after uploading for the {file_suffix}")
    
    except Exception as e:
        logger.info(f"Inside upload_file: something went wrong. Error: {e}")

    return None

logger.info("Functions defined.")

##################################### SETTING UP GOOGLE APIS AND GET THE POSTINGS THAT I ALREADY SCRAPED #####################################

# LOCAL MACHINE -- Set the environment variable for the service account credentials 
#TODO: comment for GH Actions
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials.json"

# Iterate over the number of retries
logger.info("Re-try block for Google Sheets about to start.")
for attempt in range(RETRIES):
    logger.info(f"Attempt {attempt + 1} of {RETRIES}.")

    try:
        # Authenticate using the service account
        # LOCAL MACHINE
        #TODO: comment for GH Actions
        # credentials = service_account.Credentials.from_service_account_file(os.getenv('GOOGLE_APPLICATION_CREDENTIALS'))
        # GITHUB ACTIONS
        # TODO: uncomment for GH Actions
        credentials = service_account.Credentials.from_service_account_info(json.loads(os.getenv('GOOGLE_APPLICATION_CREDENTIALS')))
        logger.info("Authenticated with Google Sheets")

        # Create service
        service = build("sheets", "v4", credentials=credentials)
        logger.info("Created service for Google Sheets")

        # Get the values from the Google Sheet with the postings
        # https://docs.google.com/spreadsheets/d/1MQxHUKmwEpeq8ObtZF6NQyyhH4nLf7S5BceFacGxN74/edit?gid=0#gid=0
        spreadsheet_postings_id = "1MQxHUKmwEpeq8ObtZF6NQyyhH4nLf7S5BceFacGxN74"
        result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_postings_id, range='A:A').execute()
        existing_postings = result.get("values", []) # Example output: [['test1'], ['abc'], ['123']]
        logger.info("Got data from Google Sheets with the postings.")

        # Get number of existing postings
        n_postings = len(existing_postings)
        logger.info(f"Number of existing compilations obtained: {n_postings}.")

        # Convert the list of lists to a set
        existing_postings = set([posting for posting_list in existing_postings for posting in posting_list])
        logger.info("Converted the list of lists to a set.")

        # Break the re-try loop if successful
        logger.info("Re-try block successful. About to break the re-try loop.")
        break

    except Exception as e:
        logger.info(f"Attempt {attempt + 1} of {RETRIES} failed. Error: {e}")

        # Check if we have retries left
        if attempt < RETRIES - 1: 
            logger.info("Sleeping before retry.")
            sleep(uniform(MIN_SLEEP_TIME, MAX_SLEEP_TIME)) 
        else:
            logger.info("All retries exhausted.")
            # Re-raise the last exception if all retries are exhausted
            raise

##################################### Initialize the driver #####################################

# driver = Driver(uc=True)
driver = Driver(uc=True, headless=True) # TODO: uncomment for GitHub Actions
logger.info("Driver initialized.")

##################################### Scrape all the counselor education jobs #####################################

# Iterate over the number of retries
logger.info("Re-try block for URL_COUNSELOR_EDUCATION_JOBS about to start.")
for attempt in range(RETRIES):
    logger.info(f"Attempt {attempt + 1} of {RETRIES}.")

    try:
        # Go to the URL
        driver.get(URL_COUNSELOR_EDUCATION_JOBS)
        logger.info(f"Driver went to {URL_COUNSELOR_EDUCATION_JOBS}.")

        # Break the re-try loop if successful
        logger.info("Re-try block successful. About to break the re-try loop.")
        break

    except Exception as e:
        logger.info(f"Attempt {attempt + 1} of {RETRIES} failed. Error: {e}")

        # Check if we have retries left
        if attempt < RETRIES - 1: 
            logger.info("Sleeping before retry.")
            sleep(uniform(MIN_SLEEP_TIME, MAX_SLEEP_TIME)) 
        else:
            logger.info("All retries exhausted.")
            # Re-raise the last exception if all retries are exhausted
            raise

# Sleep some time
sleep(uniform(MIN_SLEEP_TIME, MAX_SLEEP_TIME))
logger.info("Slept for a bit.")

# Get all the URLs to job postings
# I don't want to get all the URLs, only those including "details.cfm?JobCode=" (i.e., the job postings)
urls_job_postings = [link.get_attribute("href") for link in driver.find_elements(By.TAG_NAME, "a") if link.get_attribute("href") and "details.cfm?JobCode=" in link.get_attribute("href")]
logger.info(f"Got {len(urls_job_postings)} URLs to job postings.")

# Scroll down slowly to the end of the page
scroll_down_slowly()

##################################### Scrape individual job postings #####################################

# Create list to store the data for all the job postings
data_all_job_postings = []

logger.info("Will scrape the job postings.")
# Iterate over the URLs to job postings
for url_job_posting in urls_job_postings[:1]: # TODO: change to all the URLs
    logger.info(f"Will scrape {url_job_posting}.")

    # Extract the job code from the URL
    job_code = extract_job_code(url_job_posting)
    logger.info(f"Job code extracted: {job_code}")

    # TODO: think: can the same job be posted several times with different job codes?
    # Check whether we already have that job posting
    if job_code in existing_postings:
        logger.info(f"Already scraped posting with job code {job_code}. Skipping.")
        # Skip to the next job posting
        continue

    # Create list to store the data for a given job posting
    data_given_job_posting = []
    logger.info("Created a list to store the data for a given job posting.")

    # Append job code to the list
    data_given_job_posting.append(job_code)
    logger.info("Job code appended to the list.")

    try:

        # Go to the URL
        driver.get(url_job_posting)
        logger.info(f"Driver went to {url_job_posting}.")

        # Sleep some time
        sleep(uniform(MIN_SLEEP_TIME, MAX_SLEEP_TIME))
        logger.info("Slept for a bit.")

        # Get the source code
        source_code_job_posting = driver.page_source
        logger.info("Got the source code.")

        # Add the source code to the list
        data_given_job_posting.append(source_code_job_posting)
        logger.info("Added the source code to the list.")

        # Extract the text from the source code
        text_job_posting = extract_text(source_code_job_posting)
        logger.info("Extracted the text from the source code.")

        # Append the text to the list
        data_given_job_posting.append(text_job_posting)
        logger.info("Text appended to the list.")

        # Scroll down slowly to the end of the page
        scroll_down_slowly()

    except Exception as e:
        logger.info(f"Couldn't scrape {url_job_posting}. Error: {e}")

        # Append FAILURE to the data for given posting instead of source code and text
        data_given_job_posting.append("FAILURE")
        data_given_job_posting.append("FAILURE")
        logger.info("Appended FAILURE to the list.")

    # Append data for given posting to the data for all postings
    data_all_job_postings.append(data_given_job_posting)
    logger.info("Appended the data for a given job posting to the data for all postings.")

    # Sleep some time
    sleep(uniform(MIN_SLEEP_TIME, MAX_SLEEP_TIME))
    logger.info("Slept for a bit.")

logger.info("Scraped all the job postings.")

##################################### Quit driver #####################################

driver.quit()
logger.info("Driver quit")

####################################### WRITE NEW DATA TO GOOGLE SHEETS #######################################

# Data for the postings

# Retry block in case of failure
for attempt in range(RETRIES):

    try:
        logger.info(f"Re-try block for data for postings (Google Sheets). Attempt {attempt + 1}.")

        # Range to write the data
        range_sheet="A"+str(n_postings+1)+":A10000000"
        logger.info("Prepared range to write the data for the postings.")

        # Body of the request
        # The first element is the job code
        body={"values": [[element[0]] for element in data_all_job_postings]}
        logger.info("Prepared body of the request for the postings.")

        # Execute the request
        result = service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_postings_id,
            range=range_sheet,
            valueInputOption="USER_ENTERED",
            body=body
            ).execute()
        logger.info("Wrote new data to Google Sheets for the postings.")

        # Break the loop if successful
        logger.info("Re-try block for data for postings successful. About to break the loop.")
        break
    
    except Exception as e:
        logger.info(f"Re-try block for data for postings. Attempt {attempt + 1} failed. Error: {e}")

        if attempt < RETRIES - 1:
            logger.info("Re-try block for data for postings. Sleeping before retry.")
            sleep(5)
        else:
            logger.info("Re-try block for data for postings. All retries exhausted.")
            raise

logger.info("Wrote new data to Google Sheets for the postings.")

####################################### WRITE NEW DATA TO GOOGLE DRIVE #######################################

# Note: if there's already a file with the same name in the folder, this code will add another with the same name

# Data for the postings

# Folder ID
# https://drive.google.com/drive/u/4/folders/1oXJ1hvagM0-_Hd6Tqd9-O3BJSqfS1Y56
folder_id = "1oXJ1hvagM0-_Hd6Tqd9-O3BJSqfS1Y56" 

# Retry block in case of failure
for attempt in range(RETRIES):

    try:
        logger.info(f"Re-try block for data for the postings (Google Drive). Attempt {attempt + 1}.")

        # Authenticate using the service account (for Google Drive, not Sheets)
        service = build('drive', 'v3', credentials=credentials)
        logger.info("Created service for Google Drive.")
                    
        # Iterate over each of the job posts (list)
        for element in data_all_job_postings:
            logger.info("Iterating over each of the postings.")
            # Get the source code of the job post
            source_code = element[-2]
            logger.info("Got the source code of the post.")
            # Get the text of the job post
            text = element[-1]
            logger.info("Got the text of the post.")
            # Upload the source code to Google Drive
            upload_file(element[0], "source_code", source_code, folder_id, service, logger)
            # Upload the text to Google Drive
            upload_file(element[0], "text", text, folder_id, service, logger)
        logger.info("Wrote new data for the postings (if available) to Google Drive.")

        # Break the loop if successful
        logger.info("Re-try block for data for the postings successful. About to break the loop.")
        break

    except Exception as e:
        logger.info(f"Re-try block for data for the postings. Attempt {attempt + 1} failed. Error: {e}.")

        if attempt < RETRIES - 1:
            logger.info("Re-try block for data for the postings. Sleeping before retry.")
            sleep(5)
        else:
            logger.info("Re-try block for data for the postings. All retries exhausted.")
            raise

logger.info("Wrote new data for the postings (if available) to Google Drive.")

logger.info("Script finished.")