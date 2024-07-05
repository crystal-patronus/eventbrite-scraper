import re
import scrapy
import requests
import hashlib
import firebase_admin
from datetime import datetime
from firebase_admin import credentials, storage

cred = credentials.Certificate('./serviceAccountKey.json')
firebase_admin.initialize_app(cred, {
    'storageBucket': 'boroom.appspot.com'
})

class EventbriteSpider(scrapy.Spider):
    name = "eventbrite"
    allowed_domains = ["eventbrite.com"]

    base_url = 'https://www.eventbrite.com/d/united-states/african-american/?page={}'

    current_page = 61
    end_page = 80
    start_urls = [base_url.format(current_page)]

    def parse(self, response):
        events = response.css('section.DiscoverHorizontalEventCard-module__cardWrapper___veJo5')
        for event in events:
            try:
                event_name = event.css('section.event-card-details > div.Stack_root__1ksk7 > a > h2::text').get()
                event_link = event.css('section.event-card-details a::attr(href)').get()
                if event_link:
                    yield response.follow(event_link, self.parse_event, meta={'event_name': event_name})
            except Exception as e:
                self.logger.error(f"Error parsing event: {e}")

        if self.current_page < self.end_page:
            self.current_page += 1
            next_page_url = self.base_url.format(self.current_page)
            yield response.follow(next_page_url, self.parse)

    def parse_event(self, response):
        try:
            event_name = response.meta.get('event_name', 'N/A')
            event_date = response.css('div[data-testid="dateAndTime"] span.date-info__full-datetime::text').get()
            
            event_address_list = response.css('div[data-testid="location"] div.location-info__address *::text').getall()
            event_address = ' '.join([address.strip() for address in event_address_list if address.strip()]).strip()
            
            event_details_list = response.css('div[id="event-description"] p::text').getall()
            event_details = ' '.join([detail.strip() for detail in event_details_list if detail.strip()]).strip()
            
            event_image_url = response.css('div.event-hero-wrapper picture > img::attr(src)').get()
            event_image = self.upload_image_to_firebase(event_image_url)

            start_date, end_date = self.parse_event_date(event_date)
            phone_numbers = self.extract_phone_numbers(event_details)
            email_addresses = self.extract_email_addresses(event_details)

            yield {
                'Event Name': event_name,
                'Start Date & Time': start_date,
                'End Date & Time': end_date,
                'Address': event_address,
                'Phone': phone_numbers[0] if phone_numbers else '',
                'Email': email_addresses[0] if email_addresses else '',
                'Image': event_image
            }
        except Exception as e:
            self.logger.error(f"Error in parse_event method: {e}")
    
    def parse_event_date(self, date_str):
        months_str = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']

        if date_str is None:
            return None, None

        for month in months_str:
           if month in date_str:
                # August 24 · 12pm - August 27 · 3pm EDT
                start_index = date_str.index(month)
                date_str = date_str[start_index:]
                
                # If both the start_date and end_data exist
                if '-' in date_str:
                    # August 24 · 12pm, August 27 · 3pm, EDT
                    sub_date_strs = date_str.split(' - ')
                    start_date = sub_date_strs[0]
                    if len(sub_date_strs) == 1:
                        print(date_str)
                    end_date, timezone = self.split_date_timezone(sub_date_strs[1])

                    # 2024/8/24 - 12pm, 2024/8/27 - 3pm, EDT
                    start_date, start_time = self.split_date_time(start_date)
                    if '·' in end_date:
                        end_date, end_time = self.split_date_time(end_date)
                    else:
                        end_time = end_date.strip()
                        end_date = start_date

                    formatted_start_date = "{} - {}, {}".format(start_date, start_time, timezone)
                    formatted_end_date = "{} - {}, {}".format(end_date, end_time, timezone)

                    return formatted_start_date, formatted_end_date
                else:
                    start_date, timezone = self.split_date_timezone(date_str)

                    # 2024/8/24 - 12pm, EDT
                    start_date, start_time = self.split_date_time(start_date)
                    formatted_start_date = "{} - {}, {}".format(start_date, start_time, timezone)

                    return formatted_start_date, None
        
        return None, None
    
    def generate_blob_name(self, image_url):
        # Use a hash of the URL to create a unique file name, or use timestamp
        url_hash = hashlib.md5(image_url.encode('utf-8')).hexdigest()
        # if 'images%' in image_url:
        #     start_index = image_url.index('images%') + 7
        #     end_index = image_url.index('original')
        #     return f'images/{image_url[start_index:end_index]}.png'
        # else:
        #     return f'images/{url_hash}.png'
        return f'events/{url_hash}.png'
    
    def upload_image_to_firebase(self, image_url, destination_blob_name=None):
        if image_url is None:
            return ''

        # Download the image
        response = requests.get(image_url)
        if response.status_code == 200:
            image_data = response.content

            # Initialize Firebase Storage
            bucket = storage.bucket()
            
            # Generate a blob name if not provided
            if destination_blob_name is None:
                destination_blob_name = self.generate_blob_name(image_url)

            blob = bucket.blob(destination_blob_name)

            # Upload the image to Firebase Storage
            blob.upload_from_string(image_data, content_type='image/png')

            # Make the image publicly accessible
            blob.make_public()

            return blob.public_url
        else:
            return ''
    
    def extract_phone_numbers(self, text):
        phone_pattern = r"\b(?:\d{3}[-\.\s]??\d{3}[-\.\s]??\d{4}|\(\d{3}\)\s*\d{3}[-\.\s]??\d{4}|\d{3}[-\.\s]??\d{4})\b"
        return re.findall(phone_pattern, text)

    def extract_email_addresses(self, text):
        email_pattern = r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"
        return re.findall(email_pattern, text)
    
    def split_date_timezone(self, date_str):
        try:
            time_labels = ['am', 'pm']
            for time_label in time_labels:
                if time_label in date_str:
                    index = date_str.index(time_label) + 3
                    return date_str[0:index].strip(), date_str[index:].strip()
            return date_str, ''
        except Exception as e:
            print(f"Error in split_date_timezone: {e}")
            return date_str, ''

    def split_date_time(self, date_str):
        try:
            parsed_date_str = date_str.split(' · ')
            if len(parsed_date_str) < 2:
                raise ValueError("Input string does not contain ' · ' separator")

            parsed_date = datetime.strptime(parsed_date_str[0], "%B %d")
            parsed_date = parsed_date.replace(year=datetime.now().year)
            formatted_date = parsed_date.strftime("%m/%d/%Y")
            return formatted_date, parsed_date_str[1]
        except ValueError as ve:
            print(f"ValueError in split_date_time: {ve}")
            return '', ''
        except Exception as e:
            print(f"Error in split_date_time: {e}")
            return '', ''

