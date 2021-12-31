from Job import Job, Table

def job_1():

    data = Table('machine_events', Job.sc)

    cpu_dist = data.select(['cpus']).countByValue()

    res = '\n'.join(
        f'cpu type: {cpu_type[0]}, count: {value}' \
            for cpu_type, value in cpu_dist.items()
    )

    return res

def main():
    job = Job('name', job_1)
    job.run()
    
if __name__ == "__main__":
    main()