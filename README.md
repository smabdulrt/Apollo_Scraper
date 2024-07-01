# Apollo Crawl Spider

This project contains a Scrapy spider that crawls the Apollo.io website to gather data on companies and people based on ZIP code prefixes. The script is designed to be run with Scrapy, a popular web scraping framework for Python.

## Features

- **Login to Apollo.io**: The spider logs in to Apollo.io using provided email and password.
- **Priority-based ZIP Code Crawling**: The spider prioritizes crawling based on ZIP code prefixes and gathers information accordingly.
- **Data Extraction**: It extracts detailed information about companies and people, including their names, titles, LinkedIn profiles, and other professional details.
- **Dynamic Requests**: The spider dynamically generates and sends multiple requests to fetch data, ensuring comprehensive coverage.
- **Error Handling**: The spider includes basic error handling mechanisms to manage issues during crawling.

## Dependencies

- Python 3.7+
- Scrapy
- pandas
- openpyxl

## Setup

1. **Clone the Repository**:
   ```bash
   git clone <repository-url>
   cd <repository-directory>
   ```

2. **Install Dependencies**:
   ```bash
   pip install scrapy pandas openpyxl
   ```

3. **Configure Email and Password**:
   Open the spider file and enter your Apollo.io email and password in the `EMAIL` and `PASSWORD` variables.

   ```python
   EMAIL = "your-email@example.com"
   PASSWORD = "your-password"
   ```

4. **Prepare ZIP Code Data**:
   Ensure you have a CSV file named `List_of_ZIP_Code_prefixes_3.csv` in the same directory as the spider script. This file should contain ZIP code prefixes and their priorities.

## Running the Spider

To run the spider, use the following command:

```bash
scrapy crawl apollo_crawl
```

## Data Structure

### Company Columns
The spider extracts and stores company information in the following columns:

- `id`
- `Company`
- `Company LinkedIn`
- `Company Website`
- `Company Apollo URL`
- `Annual Revenue`
- `Industry`
- `Employees`
- `Founded Year`
- `Funding Round`
- `Company Description`
- `Company Keywords`

### People Data
The extracted details for people include:

- `First Name`
- `Last Name`
- `LinkedIn`
- `Apollo URL`
- `Title`
- `Contact Location`
- `Dates of Employment {index}`
- `Company {index}`
- `Company Location {index}`
- `Company LinkedIn {index}`
- `Company Website {index}`
- `Company Apollo URL {index}`
- `Annual Revenue {index}`
- `Industry {index}`
- `Employees {index}`
- `Founded Year {index}`
- `Funding Round {index}`
- `Company Description {index}`
- `Company Keywords {index}`

## Notes

- **Headers and Tokens**: The script dynamically handles headers and tokens required for making requests to Apollo.io.
- **Pagination**: It supports pagination to ensure all data is fetched.
- **ZIP Code Iteration**: The script iterates through ZIP code prefixes to gather data from various regions.

## Troubleshooting

- Ensure that your email and password for Apollo.io are correctly entered.
- Verify that the `List_of_ZIP_Code_prefixes_3.csv` file exists and is correctly formatted.
- Check for any network-related issues that might be causing the spider to fail in making requests.

## License

This project is licensed under the MIT License. See the LICENSE file for details.

## Acknowledgements

This project was built using Scrapy, a powerful web scraping framework for Python. Special thanks to the developers and contributors of Scrapy.

---
