import bronze
import silver
import gold
from time import sleep

if __name__ == "__main__":
    bronze.main()

    sleep(100)
    while True:
        silver.main()
        gold.main()
        sleep(600)
