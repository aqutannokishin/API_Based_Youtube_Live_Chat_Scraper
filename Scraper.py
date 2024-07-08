
import csv
import time
import os
from googleapiclient.discovery import build
from datetime import datetime
from googleapiclient.errors import HttpError

# List of YouTube API keys
api_keys = [
    'API 1',
    'API 2',
    'API 3',
    # Add more API keys here
]
current_key_index = 0

MAX_COMMENTS_PER_FILE = 1000  # Set a limit for the number of comments per file

def get_youtube_client():
    global current_key_index
    api_key = api_keys[current_key_index]
    return build('youtube', 'v3', developerKey=api_key)

youtube = get_youtube_client()

def switch_api_key():
    global current_key_index
    current_key_index = (current_key_index + 1) % len(api_keys)
    print(f"Switching to API key {current_key_index + 1}")
    return get_youtube_client()

def get_live_chat_id(video_id):
    global youtube
    while True:
        try:
            request = youtube.videos().list(
                part='liveStreamingDetails,snippet',
                id=video_id
            )
            response = request.execute()
            if 'items' not in response or len(response['items']) == 0:
                print(f"No video found with the ID: {video_id}")
                return None, None, None

            live_streaming_details = response['items'][0].get('liveStreamingDetails', {})
            snippet = response['items'][0]['snippet']
            
            live_chat_id = live_streaming_details.get('activeLiveChatId')
            channel_id = snippet['channelId']
            stream_date = snippet['publishedAt']

            if not live_chat_id:
                print(f"Live chat not available for video ID: {video_id}")
                return None, None, None

            return live_chat_id, channel_id, stream_date
        except HttpError as e:
            if e.resp.status == 403 and 'quotaExceeded' in e.content.decode():
                print("Quota exceeded for current API key.")
                youtube = switch_api_key()
            else:
                print(f"An error occurred while fetching the live chat ID: {e}")
                return None, None, None

def get_channel_name(channel_id):
    global youtube
    while True:
        try:
            request = youtube.channels().list(
                part='snippet',
                id=channel_id
            )
            response = request.execute()
            channel_name = response['items'][0]['snippet']['title']
            return channel_name
        except HttpError as e:
            if e.resp.status == 403 and 'quotaExceeded' in e.content.decode():
                print("Quota exceeded for current API key.")
                youtube = switch_api_key()
            else:
                print(f"An error occurred while fetching the channel name: {e}")
                return "unknown_channel"

def get_next_file_index(channel_name, stream_date_formatted, save_folder, suffix):
    part_index = 1
    while os.path.exists(os.path.join(save_folder, f"{channel_name}_{stream_date_formatted}_{suffix}_part{part_index}.csv")):
        part_index += 1
    return part_index

def scrape_live_chat(video_id, save_folder):
    global youtube
    comments = []
    view_data = []
    newcomers = set()
    try:
        live_chat_id, channel_id, stream_date = get_live_chat_id(video_id)
        if live_chat_id:
            channel_name = get_channel_name(channel_id)
            
            # Parse stream_date to datetime object with correct format
            stream_date_obj = datetime.strptime(stream_date, '%Y-%m-%dT%H:%M:%SZ')
            stream_date_formatted = stream_date_obj.strftime('%Y-%m-%d')
            
            comments_file_index = get_next_file_index(channel_name, stream_date_formatted, save_folder, "comments")
            viewers_file_index = get_next_file_index(channel_name, stream_date_formatted, save_folder, "viewers")
            
            request = youtube.liveChatMessages().list(
                liveChatId=live_chat_id,
                part="snippet,authorDetails",
                maxResults=2000  # Adjust maxResults as needed, 2000 is the maximum allowed
            )
            while request:
                try:
                    response = request.execute()
                    for item in response['items']:
                        snippet = item['snippet']
                        author_details = item['authorDetails']
                        
                        timestamp = snippet['publishedAt']
                        author_name = author_details['displayName']
                        comment = None
                        super_sticker = None
                        super_chat = None
                        is_member = author_details.get('isChatModerator', False) or author_details.get('isChatSponsor', False)
                        super_chat_amount = None
                        super_chat_currency = None  # Add currency field
                        new_member = None
                        membership_level = None  # Initialize membership level field
                        member_since = None  # Initialize member since field

                        if 'textMessageDetails' in snippet:
                            comment = snippet['textMessageDetails']['messageText']
                        elif 'superStickerDetails' in snippet:
                            super_sticker = snippet['displayMessage']
                        elif 'superChatDetails' in snippet:
                            super_chat = snippet['superChatDetails'].get('userComment', None)
                            if not super_chat:
                                super_chat = snippet['displayMessage']
                            super_chat_amount = snippet['superChatDetails']['amountMicros']
                            super_chat_currency = snippet['superChatDetails'].get('currency', 'USD')  # Default to USD if currency is not provided
                            # Convert super chat amount from micros to currency
                            super_chat_amount_currency = float(super_chat_amount) / 10**6  # Convert micros to currency
                            super_chat_amount = f"{super_chat_currency} {super_chat_amount_currency:.2f}"  # Concatenate currency symbol with amount
                        elif 'newSponsorDetails' in snippet:
                            new_member = author_name
                            membership_level = author_details.get('membershipLevelName', 'Unknown')  # Retrieve membership level
                            member_since = author_details.get('memberSince', 'Unknown')  # Retrieve membership duration
                            newcomers.add(new_member)

                        comments.append({
                            'timestamp': timestamp,
                            'author': author_name,
                            'comment': comment,
                            'super_sticker': super_sticker,
                            'super_chat': super_chat,
                            'is_member': is_member,
                            'super_chat_amount': super_chat_amount,
                            'new_member': new_member,  # Add new member field
                            'membership_level': membership_level,  # Add membership level field
                            'member_since': member_since  # Add member since field
                        })
                    
                    # Fetch the total viewers using the "videos" endpoint
                    total_viewers = get_total_viewers(video_id)
                    view_data.append({
                        'timestamp': timestamp,
                        'total_viewers': total_viewers,
                        'total_newcomers': len(newcomers)
                    })
                    
                    # Save comments to file if limit is reached
                    if len(comments) >= MAX_COMMENTS_PER_FILE:
                        comments_file_name = os.path.join(save_folder, f"{channel_name}_{stream_date_formatted}_comments_part{comments_file_index}.csv")
                        save_comments_to_csv(comments[:MAX_COMMENTS_PER_FILE], comments_file_name)
                        comments = comments[MAX_COMMENTS_PER_FILE:]
                        comments_file_index += 1
                    
                    # Save viewers data to file if limit is reached
                    if len(view_data) >= MAX_COMMENTS_PER_FILE:
                        viewers_file_name = os.path.join(save_folder, f"{channel_name}_{stream_date_formatted}_viewers_part{viewers_file_index}.csv")
                        save_view_data_to_csv(view_data[:MAX_COMMENTS_PER_FILE], viewers_file_name)
                        view_data = view_data[MAX_COMMENTS_PER_FILE:]
                        viewers_file_index += 1
                    
                    request = youtube.liveChatMessages().list_next(request, response)
                    time.sleep(5)  # Add a 5-second delay between requests
                except HttpError as e:
                    if e.resp.status == 403 and 'quotaExceeded' in e.content.decode():
                        print("Quota exceeded for current API key.")
                        youtube = switch_api_key()
                        request = youtube.liveChatMessages().list(
                            liveChatId=live_chat_id,
                            part="snippet,authorDetails",
                            maxResults=2000  # Adjust maxResults as needed, 2000 is the maximum allowed
                        )
                    else:
                        print(f"An error occurred: {e}")
                        break
                except KeyboardInterrupt:
                    print("Scraping interrupted by user.")
                    break

            # Save remaining comments to file
            if comments:
                comments_file_name = os.path.join(save_folder, f"{channel_name}_{stream_date_formatted}_comments_part{comments_file_index}.csv")
                save_comments_to_csv(comments, comments_file_name)
            
            # Save remaining viewers data to file
            if view_data:
                viewers_file_name = os.path.join(save_folder, f"{channel_name}_{stream_date_formatted}_viewers_part{viewers_file_index}.csv")
                save_view_data_to_csv(view_data, viewers_file_name)

            print("Scraping completed successfully.")

        else:
            print("No live chat ID found for this video.")
    except Exception as e:
        print(f"An error occurred while scraping live chat: {e}")

def save_comments_to_csv(comments, file_name):
    try:
        with open(file_name, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['timestamp', 'author', 'comment', 'super_sticker', 'super_chat', 'is_member', 'super_chat_amount', 'new_member', 'membership_level', 'member_since']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for comment in comments:
                writer.writerow(comment)
        print(f"Comments saved to {file_name}")
    except Exception as e:
        print(f"An error occurred while saving comments to CSV: {e}")

def save_view_data_to_csv(view_data, file_name):
    try:
        with open(file_name, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['timestamp', 'total_viewers', 'total_newcomers']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for data in view_data:
                writer.writerow(data)
        print(f"View data saved to {file_name}")
    except Exception as e:
        print(f"An error occurred while saving view data to CSV: {e}")

def get_total_viewers(video_id):
    global youtube
    try:
        request = youtube.videos().list(
            part='liveStreamingDetails',
            id=video_id
        )
        response = request.execute()
        live_streaming_details = response['items'][0]['liveStreamingDetails']
        total_viewers = live_streaming_details['concurrentViewers']
        return total_viewers
    except HttpError as e:
        if e.resp.status == 403 and 'quotaExceeded' in e.content.decode():
            print("Quota exceeded for current API key.")
            youtube = switch_api_key()
        else:
            print(f"An error occurred while fetching total viewers: {e}")
    except Exception as e:
        print(f"An error occurred while fetching total viewers: {e}")

# Example usage
video_id = 'Your Video ID'  # Replace with the ID of the YouTube live stream
save_folder = "C:\Path\To\File"  # Replace with your desired folder path
os.makedirs(save_folder, exist_ok=True)  # Create the folder if it doesn't exist
scrape_live_chat(video_id, save_folder)