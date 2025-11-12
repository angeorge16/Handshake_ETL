from src.fetch_current_students import extract_current_students
from src.fetch_alumni import extract_alumni
from src.fetch_mapping_codes import extract_mapping_codes

def main():
    print("\n=== Handshake ETL Pipeline ===")
    print("Please select an option:")
    print("1. Extract Current Students")
    print("2. Extract Alumni")
    print("3. Extract Mapping Codes")
    print("4. Run All ETL Jobs")
    print("5. Exit")

    choice = input("\nEnter your choice (1–5): ").strip()

    if choice == '1':
        load_type = input("Enter load type ('delta' or 'full'): ").strip().lower()
        extract_current_students(load_type)

    elif choice == '2':
        extract_alumni()

    elif choice == '3':
        extract_mapping_codes()

    elif choice == '4':
        print("\nRunning all ETL jobs sequentially...\n")

        jobs = [
            ("Current Students", lambda: extract_current_students('delta')),
            ("Alumni Students", extract_alumni),
            ("Mapping Codes", extract_mapping_codes)
        ]

        for job_name, job_func in jobs:
            print(f"\n--- Starting {job_name} ---")
            try:
                job_func()
                print(f"{job_name} completed successfully.\n")
            except Exception as e:
                print(f"Error running {job_name}: {e}\n")

        print("All ETL jobs attempted.\n")

    else:
        print("Invalid choice. Please select a valid option (1–5).")

if __name__ == "__main__":
    main()
