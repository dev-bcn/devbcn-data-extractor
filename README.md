# DevBCN Data Extractor

A Python tool that extracts speaker and session data from the DevBCN conference
via the Sessionize API and generates a consolidated CSV file with speaker
information, session titles, recording URLs, and social media links.

## Overview

This project fetches data from the DevBcn conference's Sessionize API endpoints
and processes it to create a comprehensive CSV file containing information about
speakers, their sessions, recording URLs, and social media profiles. The tool is
designed to be robust, handling various error scenarios gracefully.

## Features

- Fetches speaker data from the Sessionize API
- Fetches session data including recording URLs from the Sessionize API
- Merges the data to create a consolidated view
- Extracts social media links (LinkedIn, Twitter/X, BlueSky, Instagram)
- Handles various error scenarios gracefully
- Generates a clean CSV file with all the information

## Installation

### Prerequisites

- Python 3.11 or higher
- uv package manager

### Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/devbcn-data-extractor.git
   cd devbcn-data-extractor
   ```

2. Install dependencies:
   ```bash
   uv pip install -e .
   ```

## Usage

Run the script to generate the CSV file:

```bash
uv run process_speakers.py
```

By default, this will create a file named `devbcn-speakers.csv` in the current
directory.

You can also import the module in your own Python code:

```python
from process_speakers import main

# Generate the default CSV file
main()

# Or specify a custom output file
main(output_file="custom-filename.csv")
```

## Output Format

The generated CSV file contains the following columns:

- **Full Name**: Speaker's full name
- **Session**: Title of the session presented by the speaker
- **Recording Url**: YouTube URL for the session recording (if available)
- **LinkedIn link**: Speaker's LinkedIn profile URL (if available)
- **BlueSky link**: Speaker's BlueSky profile URL (if available)
- **Twitter link**: Speaker's Twitter/X profile URL (if available)
- **Instagram link**: Speaker's Instagram profile URL (if available)

## Testing

Run the tests to ensure everything is working correctly:

```bash
uv run -m unittest discover tests
```

## Dependencies

- [polars](https://pola.rs/): Fast DataFrame library for data processing
- [requests](https://requests.readthedocs.io/): HTTP library for API calls
- [pyarrow](https://arrow.apache.org/docs/python/): Efficient columnar memory
  format
- [openpyxl](https://openpyxl.readthedocs.io/): Library for Excel file
  operations

## License

This project is open source and available under the [MIT License](LICENSE).

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request