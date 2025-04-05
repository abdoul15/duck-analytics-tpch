import logging
import logging.handlers
from pathlib import Path
import os

def setup_logging():
    """Configure le système de logging centralisé"""
    
    # Créer le dossier logs s'il n'existe pas
    logs_dir = Path('logs')
    logs_dir.mkdir(exist_ok=True)
    
    # Format des logs
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(log_format)
    
    # Configuration du logger racine
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    
    # Handler pour la console (niveau INFO)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # Handler pour les fichiers (niveau DEBUG)
    file_handler = logging.handlers.RotatingFileHandler(
        'logs/analytics.log',
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # Handler pour les erreurs (fichier séparé)
    error_handler = logging.handlers.RotatingFileHandler(
        'logs/errors.log', 
        maxBytes=5*1024*1024,  # 5MB
        backupCount=3
    )
    error_handler.setLevel(logging.WARNING)
    error_handler.setFormatter(formatter)
    logger.addHandler(error_handler)

if __name__ == '__main__':
    setup_logging()
    logging.info("Configuration des logs initialisée")
