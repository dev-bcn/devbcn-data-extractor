import io

import polars as pl
import requests

# <editor-fold desc="Constants">
SESSIONS_API_URL = "https://sessionize.com/api/v2/xhudniix/view/Sessions"
SPEAKERS_API_URL = "https://sessionize.com/api/v2/xhudniix/view/Speakers"


# </editor-fold>

def fetch_sessions_data() -> pl.DataFrame:
    """
    Fetches and processes session data from the sessions API.

    The function retrieves session data from a specified API endpoint, structures it into
    a DataFrame, and processes it to extract relevant information such as session titles,
    recording URLs, and session IDs. It handles various exceptions such as connection errors,
    HTTP errors, timeout issues, and other generic exceptions. In case of any error during
    data fetching, an empty DataFrame is returned.

    :return: A DataFrame containing session data with the following columns:
        "SessionTitle" (str), "Recording Url" (str), and "SessionId" (int).
        If an error occurs, an empty DataFrame is returned.
    :rtype: pl.DataFrame
    """
    print(f"Fetching sessions data from '{SESSIONS_API_URL}'...")
    empty_df = pl.DataFrame({"Session": [], "Recording Url": [], "id": []})

    try:
        response = requests.get(SESSIONS_API_URL)
        response.raise_for_status()
        sessions_data = response.json()

        records = [
            {"SessionTitle": session.get("title"),
             "Recording Url": session.get("recordingUrl"),
             "SessionId": session.get("id")}
            for track in sessions_data
            for session in track.get("sessions", [])
        ]

        sessions_df = pl.DataFrame(records)

        # cast session ids from strings to ints
        sessions_df = sessions_df.with_columns(
            pl.col("SessionId").cast(pl.Int64))
        sessions_with_recordings = sessions_df.filter(
            pl.col("Recording Url").is_not_null()).height

        print(
            f"Successfully fetched data for {len(sessions_df)} sessions. {sessions_with_recordings} sessions have recording URLs.")

        return sessions_df

    except requests.exceptions.ConnectionError:
        print(
            f"Error: Could not connect to the sessions API. Continuing without recording URLs.")
        return empty_df
    except requests.exceptions.HTTPError as e:
        print(
            f"Error: HTTP error occurred when fetching sessions: {e}. Continuing without recording URLs.")
        return empty_df
    except requests.exceptions.Timeout:
        print(
            f"Error: The sessions API request timed out. Continuing without recording URLs.")
        return empty_df
    except requests.exceptions.RequestException as e:
        print(
            f"Error: An error occurred with the sessions API request: {e}. Continuing without recording URLs.")
        return empty_df
    except Exception as e:
        print(
            f"Error fetching sessions data: {e}. Continuing without recording URLs.")
        return empty_df


def fetch_speakers_data() -> pl.DataFrame:
    """
    Fetch and return data of speakers from a predefined API endpoint.

    This function performs a GET request to retrieve JSON data from the specified
    speakers API endpoint and converts it into a Polars DataFrame. If the request
    fails due to a connection error, it returns an empty DataFrame.

    :return: A Polars DataFrame containing the speakers data. If the connection to
        the API fails, an empty Polars DataFrame is returned.
    :rtype: pl.DataFrame
    """
    print(f"Fetching speakers data from '{SPEAKERS_API_URL}'...")
    try:
        response = requests.get(SPEAKERS_API_URL)
        response.raise_for_status()
        return pl.read_json(io.StringIO(response.text))
    except requests.exceptions.ConnectionError:
        return pl.DataFrame()


def main(output_file="devbcn-speakers.csv"):
    """
    Main function to fetch speaker data from Sessionize API and generate a CSV.
    
    Args:
        output_file (str): Path to the output CSV file. Defaults to "devbcn-speakers.csv".
    """
    sessions_df = fetch_sessions_data()
    speakers_df = fetch_speakers_data()

    if speakers_df.is_empty():
        print("No speaker data to process. Exiting.")
        return

    try:
        sessions_by_speakers_df = speakers_df.explode("sessions")
        sessions_by_speakers_df = sessions_by_speakers_df.with_columns(
            pl.col("sessions").struct.field("id").alias("SessionId")
        )

        merged_df = sessions_by_speakers_df.join(sessions_df, on="SessionId",
                                                 how="left")

        final_df = merged_df.select(
            pl.col("fullName").alias("Full Name"),
            pl.col("sessions").struct.field("name").alias("Session"),
            pl.col("Recording Url"),

            pl.col("links").list.eval(
                pl.element().filter(
                    pl.element().struct.field("title") == "LinkedIn")
            ).list.first().struct.field("url").alias("LinkedIn link"),

            pl.col("links").list.eval(
                pl.element().filter(
                    pl.element().struct.field("title") == "Bluesky")
            ).list.first().struct.field("url").alias("BlueSky link"),

            pl.col("links").list.eval(
                pl.element().filter(
                    pl.element().struct.field("title") == "X (Twitter)")
            ).list.first().struct.field("url").alias("Twitter link"),

            pl.col("links").list.eval(
                pl.element().filter(
                    pl.element().struct.field("title") == "Instagram")
            ).list.first().struct.field("url").alias("Instagram link"),
        )

        final_df.write_csv(output_file)

        print(
            f"Successfully generated '{output_file}' with {len(final_df)} rows.")

    except requests.exceptions.ConnectionError:
        print(
            f"Error: Could not connect to the API. Please check your internet connection.")
    except requests.exceptions.HTTPError as e:
        print(f"Error: HTTP error occurred: {e}")
    except requests.exceptions.Timeout:
        print(f"Error: The request timed out. Please try again later.")
    except requests.exceptions.RequestException as e:
        print(f"Error: An error occurred with the request: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    main()
