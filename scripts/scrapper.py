
import asyncio
import pandas as pd
import logging
from telethon import TelegramClient
from telethon.tl.functions.channels import GetParticipantsRequest
from telethon.tl.types import ChannelParticipantsSearch

# Set up logging
logging.basicConfig(
    filename='./log/telegram_scraping.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


# Your API ID and API Hash
API_ID = '21259805'
API_HASH = 'ec1c96e0b0e0455d99a75e002549e153'
# Use your phone number to login
PHONE_NUMBER = '+251970738908'

# List of channels to scrape
channels = [
    'DoctorsET',
    'lobelia4cosmetics',
    'yetenaweg',
    'EAHCI',
    # Add more channels as needed
]

async def main():
    # Create a Telegram client
    async with TelegramClient('scraper_session', API_ID, API_HASH) as client:
        # Create a list to store scraped data
        data = []

        for channel in channels:
            try:
                # Get the channel entity
                channel_entity = await client.get_entity(channel)

                # Get messages from the channel
                async for message in client.iter_messages(channel_entity, limit=100):
                    # Store message information
                    message_data = {
                        'channel': channel,
                        'message_id': message.id,
                        'date': message.date,
                        'text': message.message,
                        'media': message.media,
                    }
                    data.append(message_data)

            except Exception as e:
                print(f"Error scraping {channel}: {e}")

        # Convert the list to a DataFrame
        df = pd.DataFrame(data)

        # Save DataFrame to CSV
        df.to_csv('./data/telegram_scraped_data.csv', index=False)
        print("Data scraped and saved to data directory astelegram_scraped_data.csv")

# Run the main function
if __name__ == '__main__':
    asyncio.run(main())
