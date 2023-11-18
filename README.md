# Youtube-Video-Harvesting-and-Warehousing


This project entails the development of a Streamlit application that harvests and processes data from YouTube using Google's API, MongoDB compass, and MySQL. The app provides a user-friendly interface for data collection, storage, and analysis, facilitating users to engage with YouTube channel metrics comprehensively.

Features
•	Data Collection: Users can input YouTube channel IDs to fetch data such as channel name, subscriber count, video statistics, and more using the YouTube Data API.
•	Data Lake: The retrieved data is stored in MongoDB compass, allowing for scalable data management and retrieval.
•	Data Warehousing: Selected data can be migrated to a structured SQL database, enabling efficient data manipulation and complex queries.
•	Analysis: The Streamlit app offers various functionalities for data analysis, making it easier for users to interpret channel performance.

Approach
The project adopts a modular development approach, ensuring maintainability and portability. It includes:
•	A Streamlit app for the user interface.
•	Python scripts for interfacing with the YouTube API.
•	MongoDB compass for initial data storage.
•	MySQL Workbench for structured data warehousing and queries.
•	Data flow involves extraction from YouTube, storage in MongoDB, migration to SQL, and visualization in Streamlit.
