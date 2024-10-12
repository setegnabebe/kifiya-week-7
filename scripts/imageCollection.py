# Import the required libraries
import os
import asyncio
import logging
from telethon import TelegramClient
from telethon.tl.types import MessageMediaPhoto

# Set up logging
logging.basicConfig(
    filename='./log/image_collection.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


# Your API ID and API Hash
API_ID = '21259805'  # Replace with your API ID
API_HASH = 'ec1c96e0b0e0455d99a75e002549e153'  # Replace with your API Hash
# Use your phone number to login
PHONE_NUMBER = '+251970738908'  # Replace with your phone number

# List of channels to scrape
channels = [
    'Chemed',
    'lobelia4cosmetics',
]

# Directory to save images
image_dir = 'downloaded_images'
os.makedirs(image_dir, exist_ok=True)  # Create directory if it doesn't exist

async def main():
    # Create a Telegram client
    async with TelegramClient('scraper_session', API_ID, API_HASH) as client:
        # Login if needed
        await client.start(PHONE_NUMBER)

        for channel in channels:
            try:
                # Get the channel entity
                channel_entity = await client.get_entity(channel)

                # Get messages from the channel
                async for message in client.iter_messages(channel_entity):
                    # Check if the message contains media
                    if message.media and isinstance(message.media, MessageMediaPhoto):
                        # Download the image
                        file_name = f"{channel}_{message.id}.jpg"
                        file_path = os.path.join(image_dir, file_name)
                        await client.download_media(message.media, file=file_path)
                        print(f"Downloaded: {file_path}")

            except Exception as e:
                print(f"Error scraping {channel}: {e}")

# Run the main function within an event loop
if __name__ == "__main__":
    asyncio.run(main())
