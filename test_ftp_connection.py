import logging
from ftplib import FTP_TLS
import ssl

# Set up logging for debugging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("ftplib")

# FTP server configuration
ftp_config = {
    "host": "127.0.0.1",
    "port": 21,  # Default FTP port
    "user": "nav_auto",
    "password": "hola",
    "directory": "/1"
}


def connect_with_ftps():
    """Connect to an FTP server using FTPS (explicit TLS) with proper TLS session resumption."""
    print("Connecting to FTP server with FTPS...")
    try:
        # Create a secure TLS context with session resumption enabled
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE

        # Initialize FTPS connection
        with FTP_TLS(context=context) as ftps:
            ftps.debugging = 2  # Enable debugging output
            ftps.connect(host=ftp_config['host'], port=ftp_config['port'])
            print("Authenticating TLS...")
            ftps.auth()  # Perform explicit TLS handshake
            ftps.prot_p()  # Enable TLS for data connections

            print("Logging in...")
            ftps.login(user=ftp_config['user'], passwd=ftp_config['password'])

            print("Setting passive mode...")
            ftps.set_pasv(True)

            print("Changing to target directory...")
            ftps.cwd(ftp_config['directory'])

            print("Directory listing:")
            ftps.dir()  # List files in the directory

    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    connect_with_ftps()
