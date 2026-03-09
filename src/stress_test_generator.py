import random
import pandas as pd
from faker import Faker

fake = Faker()

N = 10000  # adjust later

POSITIONS = [
    "Sales Associate",
    "Store Manager",
    "HR Generalist",
    "Payroll Clerk",
    "Sales Lead",
]

STATES = ["TX", "CA", "FL", "NY"]

def generate_old_data(n):
    rows = []
    for i in range(n):
        first = fake.first_name()
        last = fake.last_name()
        state = random.choice(STATES)
        city = fake.city()
        salary = random.randint(40000, 90000)

        rows.append({
            "first_name": first,
            "last_name": last,
            "position": random.choice(POSITIONS),
            "dob": fake.date_of_birth(minimum_age=22, maximum_age=60),
            "hire_date": fake.date_between(start_date="-10y", end_date="today"),
            "location": f"{city} {state}",
            "salary": salary,
            "payrate": "",
            "worker_status": "active",
            "worker_type": "regular",
            "district": random.choice(["North", "South"]),
            "last4_ssn": str(random.randint(1000, 9999)),
            "address": fake.street_address() + f", {city} {state}",
            "worker_id": f"wd{i:05d}"
        })

    return pd.DataFrame(rows)


def generate_new_data(old_df):
    new_df = old_df.copy()

    # Salary changes 3%
    for idx in random.sample(range(len(new_df)), int(len(new_df) * 0.03)):
        new_df.at[idx, "salary"] += random.randint(1000, 5000)

    # Status changes 2%
    for idx in random.sample(range(len(new_df)), int(len(new_df) * 0.02)):
        new_df.at[idx, "worker_status"] = "terminated"

    # Title changes 1%
    for idx in random.sample(range(len(new_df)), int(len(new_df) * 0.01)):
        new_df.at[idx, "position"] = "Senior " + new_df.at[idx, "position"]

    # Random state drop 2%
    for idx in random.sample(range(len(new_df)), int(len(new_df) * 0.02)):
        new_df.at[idx, "location"] = new_df.at[idx, "location"].split()[0]

    # Create duplicate-name collision 2%
    for idx in random.sample(range(len(new_df)), int(len(new_df) * 0.02)):
        dup = new_df.iloc[idx].copy()
        dup["worker_id"] = dup["worker_id"] + "_dup"
        new_df = pd.concat([new_df, pd.DataFrame([dup])], ignore_index=True)

    return new_df


if __name__ == "__main__":
    old_df = generate_old_data(N)
    new_df = generate_new_data(old_df)

    old_df.to_csv("data/adp.csv", index=False)
    new_df.to_csv("data/workday.csv", index=False)

    print("Stress test data generated.")

