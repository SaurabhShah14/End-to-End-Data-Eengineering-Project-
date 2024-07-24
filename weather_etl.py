import requests
import csv
import s3fs

# Set up your S3 credentials
AWS_ACCESS_KEY_ID = 'Your_AWS_ACCESS_KEY'
AWS_SECRET_ACCESS_KEY = 'Your_AWS_SECRET_ACCESS_KEY'

# Set up your S3 bucket and file path
bucket_name = 'dataengineering-project2'
file_path = 'weather_data.csv'

# Set up your API endpoint
api_endpoint = "https://weatherapi-com.p.rapidapi.com/current.json"
headers = {
    "x-rapidapi-key": "46d1fcc8c8mshe4abc36dd2369e4p15d1e7jsnbe724d8432dd",
    "x-rapidapi-host": "weatherapi-com.p.rapidapi.com"
}

# Create a CSV writer
fs = s3fs.S3FileSystem(anon=False, key=AWS_ACCESS_KEY_ID, secret=AWS_SECRET_ACCESS_KEY)
with fs.open(f's3://{bucket_name}/{file_path}', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)

    # Write the header row
    writer.writerow(['location', 'region', 'country', 'lat', 'lon', 'temp_c', 'condition', 'wind_kph', 'wind_mph', 'humidity', 'cloud'])

    # Iterate over the list of coordinates
    coordinates = [
    "53.1,-0.13",  # Original coordinate
    "39.9, 116.4",  # Beijing, China
    "31.2, 121.5",  # Shanghai, China
    "23.1, 113.3",  # Guangzhou, China
    "35.7, 139.8",  # Tokyo, Japan
    "34.7, 135.5",  # Osaka, Japan
    "35.2, 136.9",  # Nagoya, Japan
    "28.6, 77.2",  # New Delhi, India
    "19.1, 72.8",  # Mumbai, India
    "12.9, 77.6",  # Bangalore, India
    "1.3, 103.8",  # Singapore
    "13.7, 100.5",  # Bangkok, Thailand
    "3.1, 101.7",  # Kuala Lumpur, Malaysia
    "6.2, 106.8",  # Jakarta, Indonesia
    "37.6, 126.9",  # Seoul, Korea
    "35.1, 129.0",  # Busan, Korea
    "40.7, -74.0",  # New York City, USA
    "34.0, -118.2",  # Los Angeles, USA
    "41.8, -87.7",  # Chicago, USA
    "29.7, -95.4",  # Houston, USA
    "25.8, -80.2",  # Miami, USA
    "48.8, 2.3",  # Paris, France
    "51.5, -0.1",  # London, UK
    "52.5, 13.4",  # Berlin, Germany
    "45.4, 9.2",  # Milan, Italy
    "40.4, -3.7",  # Madrid, Spain
    "48.8, 9.2",  # Stuttgart, Germany
    "50.8, 4.3",  # Brussels, Belgium
    "52.3, 4.9",  # Amsterdam, Netherlands
    "59.9, 10.7",  # Oslo, Norway
    "55.7, 12.6",  # Copenhagen, Denmark
    "60.2, 24.9",  # Helsinki, Finland
    "37.9, 23.7",  # Athens, Greece
    "41.9, 12.5",  # Rome, Italy
    "46.2, 6.1",  # Geneva, Switzerland
    "50.1, 8.7",  # Frankfurt, Germany
    "49.2, 16.6",  # Vienna, Austria
    "42.7, -83.2",  # Detroit, USA
    "43.6, -79.4",  # Toronto, Canada
    "45.5, -73.6",  # Montreal, Canada
    "38.9, -77.0",  # Washington D.C., USA
    "32.7, -117.1",  # San Diego, USA
    "37.7, -122.4",  # San Francisco, USA
    "45.5, -122.7",  # Portland, USA
    "47.6, -122.3",  # Seattle, USA
    "34.0, -118.2",  # Phoenix, USA
    "39.7, -104.9",  # Denver, USA
    "29.7, -95.4",  # Dallas, USA
    "35.8, -78.6",  # Raleigh, USA
    "33.7, -84.4",  # Atlanta, USA
    "41.8, -87.7",  # Indianapolis, USA
    "39.1, -84.5",  # Cincinnati, USA
    "42.3, -71.0",  # Boston, USA
    "40.7, -74.0",  # Philadelphia, USA
    "38.9, -77.0",  # Baltimore, USA
    "-33.8, 151.2",  # Sydney, Australia
    "-37.8, 144.9",  # Melbourne, Australia
    "-27.4, 153.0",  # Brisbane, Australia
    "-31.9, 115.8",  # Perth, Australia
    ]

    for coord in coordinates:
        querystring = {"q": coord}
        response = requests.get(api_endpoint, headers=headers, params=querystring)

        if response.status_code == 200:
            data = response.json()
            writer.writerow([
                data['location']['name'],
                data['location']['region'],
                data['location']['country'],
                data['location']['lat'],
                data['location']['lon'],
                data['current']['temp_c'],
                data['current']['condition']['text'],
                data['current']['wind_kph'],
                data['current']['wind_mph'],
                data['current']['humidity'],
                data['current']['cloud']
            ])
        else:
            print(f'Error: {response.status_code}')

print(f'Data saved to s3://{bucket_name}/{file_path}')