# AWS Key Pair Management

This project provides functionality to create and manage AWS key pairs for multiple users. It includes features for creating key pairs, saving them to files, and storing them in AWS Secrets Manager.

## Features

- Create AWS key pairs for multiple users
- Save private and public keys to files
- Store keys in AWS Secrets Manager
- Comprehensive test suite
- Example usage script

## Prerequisites

- Python 3.7+
- AWS CLI configured with a profile named 'latest'
- Required permissions for EC2 and Secrets Manager

## Installation

1. Clone the repository
2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### Running Tests

To run the test suite:
```bash
pytest test/
```

### Creating Key Pairs

1. Create a `username.json` file with the following format:
```json
{
    "usernames": [
        "user1",
        "user2",
        "user3"
    ]
}
```

2. Run the script:
```bash
python create_key_pair.py username.json --output-dir ./keys
```

### Example Script

An example script is provided in the `_example` directory. To run it:
```bash
python _example/run_key_pair_creation.py
```

This will:
1. Create a sample username.json file
2. Create key pairs for the sample users
3. Save the keys to files
4. Store the keys in AWS Secrets Manager

## Project Structure

```
.
├── create_key_pair.py      # Main implementation
├── requirements.txt        # Project dependencies
├── test/                  # Test directory
│   └── test_create_key_pair.py
├── _example/             # Example usage
│   ├── run_key_pair_creation.py
│   └── username.json
└── README.md
```

## Security Notes

- Private keys are saved with 600 permissions (user read/write only)
- Keys are stored in AWS Secrets Manager for secure access
- The script uses the 'latest' AWS profile for authentication

## Error Handling

The script includes comprehensive error handling for:
- Invalid JSON files
- Duplicate key pairs
- AWS service errors
- File system errors

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request