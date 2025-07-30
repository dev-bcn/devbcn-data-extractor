import io
import json
import os
import sys
import tempfile
import unittest
from unittest.mock import patch, MagicMock

import polars as pl
import requests

# Add the parent directory to sys.path to import process_speakers
sys.path.insert(0,
                os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import process_speakers
from process_speakers import SESSIONS_API_URL, SPEAKERS_API_URL


class TestProcessSpeakers(unittest.TestCase):
    def setUp(self):
        # Sample JSON data for testing speakers
        self.sample_speakers_json = '''[
            {
                "id": "1e0a598b-2843-4a2b-b894-793e9fcaa999",
                "firstName": "Alex",
                "lastName": "Shershebnev",
                "fullName": "Alex Shershebnev",
                "bio": "Sample bio",
                "tagLine": "Head of ML/DevOps at Zencoder",
                "profilePicture": "https://example.com/image.jpg",
                "sessions": [
                    {
                        "id": 826253,
                        "name": "Developing production-ready apps in collaboration with AI Agents"
                    }
                ],
                "isTopSpeaker": false,
                "links": [
                    {
                        "title": "LinkedIn",
                        "url": "https://linkedin.com/in/shershebnev",
                        "linkType": "LinkedIn"
                    },
                    {
                        "title": "Instagram",
                        "url": "https://instagram.com/shershebnev",
                        "linkType": "Instagram"
                    }
                ],
                "questionAnswers": [],
                "categories": []
            },
            {
                "id": "f2e1dff5-e8b9-4b4b-a8e4-4f5c6c61158f",
                "firstName": "Abdel",
                "lastName": "Sghiouar",
                "fullName": "Abdel Sghiouar",
                "bio": "Sample bio",
                "tagLine": "Cloud Developer Advocate",
                "profilePicture": "https://example.com/image2.jpg",
                "sessions": [
                    {
                        "id": 834677,
                        "name": "Yes you can run LLMs on Kubernetes"
                    }
                ],
                "isTopSpeaker": false,
                "links": [
                    {
                        "title": "X (Twitter)",
                        "url": "https://www.twitter.com/boredabdel",
                        "linkType": "Twitter"
                    },
                    {
                        "title": "LinkedIn",
                        "url": "https://www.linkedin.com/in/sabdelfettah/",
                        "linkType": "LinkedIn"
                    },
                    {
                        "title": "Bluesky",
                        "url": "https://bsky.app/profile/abdel.bsky.social",
                        "linkType": "Bluesky"
                    }
                ],
                "questionAnswers": [],
                "categories": []
            }
        ]'''

        # Sample JSON data for testing sessions
        self.sample_sessions_json = '''[
            {
                "groupId": 159116,
                "groupName": "Java",
                "sessions": [
                    {
                        "id": "826253",
                        "title": "Developing production-ready apps in collaboration with AI Agents",
                        "description": "Sample description",
                        "startsAt": "2023-07-03T11:10:00",
                        "endsAt": "2023-07-03T12:00:00",
                        "isServiceSession": false,
                        "room": "Java",
                        "liveUrl": null,
                        "recordingUrl": "https://www.youtube.com/embed/abc123",
                        "status": "Accepted",
                        "isInformed": true
                    }
                ]
            },
            {
                "groupId": 159117,
                "groupName": "Kubernetes",
                "sessions": [
                    {
                        "id": "834677",
                        "title": "Yes you can run LLMs on Kubernetes",
                        "description": "Sample description",
                        "startsAt": "2023-07-03T13:10:00",
                        "endsAt": "2023-07-03T14:00:00",
                        "isServiceSession": false,
                        "room": "Kubernetes",
                        "liveUrl": null,
                        "recordingUrl": "https://www.youtube.com/embed/def456",
                        "status": "Accepted",
                        "isInformed": true
                    }
                ]
            }
        ]'''

        # Create a temporary directory for test output
        self.test_dir = tempfile.TemporaryDirectory()

        # Set a test output file path
        self.test_output_file = os.path.join(self.test_dir.name,
                                             "test-speakers.csv")

        # Create a mapping of session IDs to recording URLs for testing
        self.session_recordings = {
            "826253": "https://www.youtube.com/embed/abc123",
            "834677": "https://www.youtube.com/embed/def456"
        }

    def tearDown(self):
        # Clean up the temporary directory
        self.test_dir.cleanup()

    @patch('requests.get')
    def test_successful_api_call(self, mock_get):
        # Mock the API responses for both sessions and speakers
        def mock_api_response(*args, **kwargs):
            url = args[0]
            mock_response = MagicMock()

            if url == SESSIONS_API_URL:
                mock_response.text = self.sample_sessions_json
                mock_response.json.return_value = json.loads(
                    self.sample_sessions_json)
            elif url == SPEAKERS_API_URL:
                mock_response.text = self.sample_speakers_json

            mock_response.raise_for_status.return_value = None
            return mock_response

        mock_get.side_effect = mock_api_response

        # Call the main function with our test output file
        process_speakers.main(output_file=self.test_output_file)

        # Verify that the output file was created
        self.assertTrue(os.path.exists(self.test_output_file))

        # Read the CSV file and verify its contents
        df = pl.read_csv(self.test_output_file)

        # Check that the DataFrame has the expected number of rows
        # We have 2 speakers, but one has 1 session and the other has 1 session,
        # so we expect 2 rows after exploding the sessions
        self.assertEqual(len(df), 2)

        # Check that the DataFrame has the expected columns
        expected_columns = ["Full Name", "Session", "Recording Url",
                            "LinkedIn link", "BlueSky link", "Twitter link",
                            "Instagram link"]
        self.assertListEqual(df.columns, expected_columns)

        # Check specific values
        self.assertIn("Alex Shershebnev", df["Full Name"].to_list())
        self.assertIn("Abdel Sghiouar", df["Full Name"].to_list())
        self.assertIn(
            "Developing production-ready apps in collaboration with AI Agents",
            df["Session"].to_list())
        self.assertIn("Yes you can run LLMs on Kubernetes",
                      df["Session"].to_list())

        # Check recording URLs
        self.assertIn("https://www.youtube.com/embed/abc123",
                      df["Recording Url"].to_list())
        self.assertIn("https://www.youtube.com/embed/def456",
                      df["Recording Url"].to_list())

        # Check social media links
        self.assertIn("https://linkedin.com/in/shershebnev",
                      df["LinkedIn link"].to_list())
        self.assertIn("https://www.linkedin.com/in/sabdelfettah/",
                      df["LinkedIn link"].to_list())
        self.assertIn("https://bsky.app/profile/abdel.bsky.social",
                      df["BlueSky link"].to_list())
        self.assertIn("https://www.twitter.com/boredabdel",
                      df["Twitter link"].to_list())
        self.assertIn("https://instagram.com/shershebnev",
                      df["Instagram link"].to_list())

    @patch('requests.get')
    def test_connection_error_sessions(self, mock_get):
        # Mock a connection error for the sessions API
        def mock_api_response(*args, **kwargs):
            url = args[0]
            if url == SESSIONS_API_URL:
                raise requests.exceptions.ConnectionError("Connection error")
            elif url == SPEAKERS_API_URL:
                # For speakers API, return a valid response
                mock_response = MagicMock()
                mock_response.text = self.sample_speakers_json
                mock_response.raise_for_status.return_value = None
                return mock_response

        mock_get.side_effect = mock_api_response

        # Call the main function with our test output file and capture stdout
        with patch('sys.stdout', new=io.StringIO()) as fake_stdout:
            process_speakers.main(output_file=self.test_output_file)
            output = fake_stdout.getvalue()

        # Verify the error message
        self.assertIn("Error: Could not connect to the sessions API", output)

        # The process continues with empty session recordings, but no output file is created
        # because the join operation will not find matching session IDs
        self.assertFalse(os.path.exists(self.test_output_file))

    @patch('requests.get')
    def test_connection_error_speakers(self, mock_get):
        # Mock a connection error for the speakers API
        def mock_api_response(*args, **kwargs):
            url = args[0]
            if url == SESSIONS_API_URL:
                # For sessions API, return a valid response
                mock_response = MagicMock()
                mock_response.text = self.sample_sessions_json
                mock_response.json.return_value = json.loads(
                    self.sample_sessions_json)
                mock_response.raise_for_status.return_value = None
                return mock_response
            elif url == SPEAKERS_API_URL:
                raise requests.exceptions.ConnectionError("Connection error")

        mock_get.side_effect = mock_api_response

        # Call the main function with our test output file and capture stdout
        with patch('sys.stdout', new=io.StringIO()) as fake_stdout:
            process_speakers.main(output_file=self.test_output_file)
            output = fake_stdout.getvalue()

        # Verify that the sessions data was fetched successfully
        self.assertIn("Successfully fetched data for", output)

        # Verify that the speakers API error was handled and the process exited
        self.assertIn("No speaker data to process. Exiting.", output)

        # Verify that the output file was not created
        self.assertFalse(os.path.exists(self.test_output_file))

    @patch('requests.get')
    def test_http_error_sessions(self, mock_get):
        # Mock an HTTP error for the sessions API
        def mock_api_response(*args, **kwargs):
            url = args[0]
            mock_response = MagicMock()

            if url == SESSIONS_API_URL:
                mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
                    "404 Client Error")
            elif url == SPEAKERS_API_URL:
                # For speakers API, return a valid response
                mock_response.text = self.sample_speakers_json
                mock_response.raise_for_status.return_value = None

            return mock_response

        mock_get.side_effect = mock_api_response

        # Call the main function with our test output file and capture stdout
        with patch('sys.stdout', new=io.StringIO()) as fake_stdout:
            process_speakers.main(output_file=self.test_output_file)
            output = fake_stdout.getvalue()

        # Verify the error message
        self.assertIn("Error: HTTP error occurred when fetching sessions",
                      output)

        # The process continues with empty session recordings, but no output file is created
        # because the join operation will not find matching session IDs
        self.assertFalse(os.path.exists(self.test_output_file))

    @patch('requests.get')
    def test_timeout_error_sessions(self, mock_get):
        # Mock a timeout error for the sessions API
        def mock_api_response(*args, **kwargs):
            url = args[0]
            if url == SESSIONS_API_URL:
                raise requests.exceptions.Timeout("Timeout error")
            elif url == SPEAKERS_API_URL:
                # For speakers API, return a valid response
                mock_response = MagicMock()
                mock_response.text = self.sample_speakers_json
                mock_response.raise_for_status.return_value = None
                return mock_response

        mock_get.side_effect = mock_api_response

        # Call the main function with our test output file and capture stdout
        with patch('sys.stdout', new=io.StringIO()) as fake_stdout:
            process_speakers.main(output_file=self.test_output_file)
            output = fake_stdout.getvalue()

        # Verify the error message
        self.assertIn("Error: The sessions API request timed out", output)

        # The process continues with empty session recordings, but no output file is created
        # because the join operation will not find matching session IDs
        self.assertFalse(os.path.exists(self.test_output_file))

    @patch('process_speakers.fetch_speakers_data')
    @patch('process_speakers.fetch_sessions_data')
    def test_http_error_speakers(self, mock_fetch_sessions,
                                 mock_fetch_speakers):
        # Mock the sessions data
        sessions_df = pl.DataFrame({
            "SessionTitle": ["Session 1", "Session 2"],
            "Recording Url": ["https://example.com/1", "https://example.com/2"],
            "SessionId": [1, 2]
        })
        mock_fetch_sessions.return_value = sessions_df

        # Mock the speakers data to return an empty DataFrame (as if there was an error)
        mock_fetch_speakers.return_value = pl.DataFrame()

        # Call the main function with our test output file and capture stdout
        with patch('sys.stdout', new=io.StringIO()) as fake_stdout:
            process_speakers.main(output_file=self.test_output_file)
            output = fake_stdout.getvalue()

        # Verify that the process exited early due to empty speakers data
        self.assertIn("No speaker data to process. Exiting.", output)

        # Verify that the output file was not created
        self.assertFalse(os.path.exists(self.test_output_file))

    @patch('process_speakers.fetch_speakers_data')
    @patch('process_speakers.fetch_sessions_data')
    def test_timeout_error_speakers(self, mock_fetch_sessions,
                                    mock_fetch_speakers):
        # Mock the sessions data
        sessions_df = pl.DataFrame({
            "SessionTitle": ["Session 1", "Session 2"],
            "Recording Url": ["https://example.com/1", "https://example.com/2"],
            "SessionId": [1, 2]
        })
        mock_fetch_sessions.return_value = sessions_df

        # Mock the speakers data to return an empty DataFrame (as if there was an error)
        mock_fetch_speakers.return_value = pl.DataFrame()

        # Call the main function with our test output file and capture stdout
        with patch('sys.stdout', new=io.StringIO()) as fake_stdout:
            process_speakers.main(output_file=self.test_output_file)
            output = fake_stdout.getvalue()

        # Verify that the process exited early due to empty speakers data
        self.assertIn("No speaker data to process. Exiting.", output)

        # Verify that the output file was not created
        self.assertFalse(os.path.exists(self.test_output_file))


if __name__ == '__main__':
    unittest.main()
