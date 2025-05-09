from typing import Dict, List, Optional
from dataclasses import dataclass


@dataclass
class FTPConfig:
    host: str
    user: str
    password: str
    directory: str = "/"


@dataclass
class SMTPConfig:
    host: str
    port: int
    user: str
    password: str
    use_tls: bool = True


@dataclass
class GoogleDriveConfig:
    credentials_path: str
    input_folder_id: Optional[str] = None
    morningstar_output_folder_id: Optional[str] = None
    six_output_folder_id: Optional[str] = None


@dataclass
class AppConfig:
    mode: str = "local"  # "local" or "remote"
    ftp_configs: Dict[str, FTPConfig] = None
    smtp_config: Optional[SMTPConfig] = None
    drive_config: Optional[GoogleDriveConfig] = None
    db_connection_string: str = 'sqlite:///nav_data.db'
    max_workers: int = 5
    input_dir: str = "input"
    output_dir: str = "output"
    template_dir: str = "input/template"
    log_level: str = "INFO"  # "DEBUG", "INFO", "WARNING", "ERROR"

    @classmethod
    def from_dict(cls, config_dict: Dict) -> 'AppConfig':
        """Create an AppConfig instance from a dictionary"""
        ftp_configs = {}
        if config_dict.get('ftp_configs'):
            for emitter, config in config_dict['ftp_configs'].items():
                ftp_configs[emitter] = FTPConfig(**config)

        smtp_config = None
        if config_dict.get('smtp_config'):
            smtp_config = SMTPConfig(**config_dict['smtp_config'])

        drive_config = None
        if config_dict.get('drive_config'):
            drive_config = GoogleDriveConfig(**config_dict['drive_config'])

        return cls(
            mode=config_dict.get('mode', 'local'),
            ftp_configs=ftp_configs,
            smtp_config=smtp_config,
            drive_config=drive_config,
            db_connection_string=config_dict.get(
                'db_connection_string', 'sqlite:///nav_data.db'),
            max_workers=config_dict.get('max_workers', 5),
            input_dir=config_dict.get('input_dir', 'input'),
            output_dir=config_dict.get('output_dir', 'output'),
            template_dir=config_dict.get('template_dir', 'input/template'),
            log_level=config_dict.get('log_level', 'INFO')
        )


# Default configurations for emitters
DEFAULT_FTP_CONFIGS = {
    "ETPCAP2": {
        "host": "",
        "user": "nav_auto",
        "password": "hola",
        "directory": "/1"
    },
    "HFMX": {
        "host": "teo.superhosting.bg",
        "user": "data@hfmxdacseries.com",
        "password": "BF0*5bZIRZK^",
        "directory": "/"
    },
    "IACAP": {
        "host": "omar.superhosting.bg",
        "user": "data@iacapitalplc.com",
        "password": "BF0*5bZIRZK^",
        "directory": "/"
    },
    "CIX": {
        "host": "mini.superhosting.bg",
        "user": "data@cixdac.com",
        "password": "BF0*5bZIRZK^",
        "directory": "/"
    },
    "DCXPD": {
        "host": "mini.superhosting.bg",
        "user": "data@dcxpd.com",
        "password": "9RF#c[tCq}rT",
        "directory": "/"
    }
}

# Template file patterns
FILE_PATTERNS = {
    "standard": "CAS_Flexfunds_NAV_{date_str} {emitter}.csv",
    "hybrid": "CAS_Flexfunds_NAV_{date_str} Wrappers Hybrid {emitter}.csv",
    "loan": "CAS_Flexfunds_NAV_{date_str} Loan {emitter}.csv"
}

# List of supported emitters
EMITTERS = ["ETPCAP2", "HFMX", "IACAP", "CIX", "DCXPD"]

# List of supported file types
FILE_TYPES = ["standard", "hybrid", "loan"]
