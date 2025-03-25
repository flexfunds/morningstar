from models import Base, init_db

if __name__ == "__main__":
    Session = init_db()
    print("Database tables created successfully!")
