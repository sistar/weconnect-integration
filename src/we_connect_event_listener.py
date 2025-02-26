import logging
import os
from time import sleep
from logging.handlers import RotatingFileHandler

from dotenv import load_dotenv
from pymongo import DESCENDING, MongoClient
from weconnect import addressable, weconnect
from weconnect.weconnect_errors import ErrorEventType
from weconnect.errors import RetrievalError, TooManyRequestsError, SetterError, ControlError, AuthentificationError, TemporaryAuthentificationError, APICompatibilityError, APIError
from weconnect.elements.access_status import AccessStatus
# Create logs directory if it doesn't exist
log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs')
os.makedirs(log_dir, exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        RotatingFileHandler(
            os.path.join(log_dir, 'weconnect.log'),
            maxBytes=1024*1024,  # 1MB
            backupCount=5
        ),
        logging.StreamHandler()  # Also log to console
    ]
)

logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

# Get username and password from environment variables
username = os.getenv("WE_CON_USER")
password = os.getenv("PASSWORD")

# Ensure username and password are not None
if username is None or password is None:
    raise ValueError("Username and password must be set in environment variables")
uri = os.getenv("MONGODB_URI")

# Create a new client and connect to the server
client = MongoClient(uri)
db = client.get_database("weconnect")
collection = db["vehicle_events"]

nominal = True

# Define a global flag
reconnect_required = False

def main_loop():
    """Main event loop for WeConnect integration.

    This function establishes and maintains a connection to the WeConnect service, handling
    authentication, event registration, and periodic updates. It implements automatic
    reconnection on errors and graceful disconnection.

    The loop:
    1. Initializes WeConnect connection with provided credentials
    2. Performs login and registers error/event observers
    3. Runs update loop every 5 minutes until reconnection is required
    4. Handles various API exceptions and connection errors
    5. Performs cleanup and disconnection
    6. Waits 10 seconds before attempting reconnection

    Global Variables:
        reconnect_required (bool): Flag indicating if reconnection is needed
        username (str): WeConnect account username
        password (str): WeConnect account password

    Raises:
        ConnectionError: If connection to WeConnect service fails
        RetrievalError: If data retrieval fails
        TooManyRequestsError: If API rate limit is exceeded
        SetterError: If setting values fails
        ControlError: If control operations fail
        AuthentificationError: If authentication fails
        TemporaryAuthentificationError: If temporary auth issues occur
        APICompatibilityError: If API version is incompatible
        APIError: For general API errors

    Note:
        Function runs indefinitely until process is terminated
    """
    global reconnect_required # pylint: disable=global-statement
    while True:
        try:
            logger.info("Initialize WeConnect user:%s pass:%s", username, password)
            we_connect = weconnect.WeConnect(
                username=username,  # type: ignore
                password=password,  # type: ignore
                updateAfterLogin=False,
                loginOnInit=False,
            )
            logger.info("Login")
            we_connect.login()
            we_connect.addErrorObserver(on_we_connect_error, ErrorEventType.ALL)
            logger.info("Register for events")
            we_connect.addObserver(
                on_we_connect_event,
                addressable.AddressableLeaf.ObserverEvent.VALUE_CHANGED
                | addressable.AddressableLeaf.ObserverEvent.ENABLED
                | addressable.AddressableLeaf.ObserverEvent.DISABLED,
            )
            logger.info("Update")
            reconnect_required = False  # reset before update loop
            while not reconnect_required:
                we_connect.update()
                sleep(300)
        except (ConnectionError, RetrievalError,TooManyRequestsError,SetterError,ControlError,AuthentificationError,TemporaryAuthentificationError,APICompatibilityError,APIError) as e:
            logger.error("Exception in main loop: %s", e)
        finally:
            try:
                we_connect.disconnect()
                logger.info("WeConnect connection terminated.")
            except (ConnectionError, RetrievalError,TooManyRequestsError,SetterError, ControlError,AuthentificationError,TemporaryAuthentificationError,APICompatibilityError,APIError) as logout_error:
                logger.error("Error during logout: %s", logout_error)
        logger.info("Reconnecting in 10 seconds...")
        sleep(10)

def on_we_connect_error(error):
    """
    Handle WeConnect error events by logging the error and triggering reconnection.

    This callback function is invoked when a WeConnect error occurs. It logs the error
    and sets a global flag to indicate that a reconnection to the WeConnect service 
    is required.

    Args:
        error: The error that occurred during WeConnect operation.
            Can be any error type raised by the WeConnect client.

    Global Variables:
        reconnect_required (bool): Flag modified to trigger reconnection logic.

    Returns:
        None
    """
    logger.error("Error: %s", error)
    # Signal that a reconnect is required
    global reconnect_required # pylint: disable=global-statement
    reconnect_required = True

def on_we_connect_event(element, flags):
    """
    Event handler for WeConnect events that processes changes in addressable attributes.
    Added exception handling to log unexpected errors.
    """
    try:
        if isinstance(element, addressable.AddressableAttribute):
            if flags & addressable.AddressableLeaf.ObserverEvent.ENABLED:
                logger.debug("New attribute is available: %s: %s", element.getGlobalAddress(), element.value)
            elif flags & addressable.AddressableLeaf.ObserverEvent.VALUE_CHANGED:
                ga = element.getGlobalAddress()
                logger.debug("Value changed: %s: %s last change: %s", ga, element.value, element.lastChange)
                # Handle token or attribute specific logic here...
                if "access/accessStatus/" in ga:
                    if ga.endswith("doors/frontLeft/lockState"):
                        # Convert enum to string if needed
                        door_lock_status = element.value.value if hasattr(element.value, "value") else element.value
                        if element.value == AccessStatus.Door.LockState.LOCKED:
                            logger.info("Door is locked")
                            entry = {
                                "doorLockStatus": door_lock_status,
                                "status": "door lock status",
                                "timestamp": element.lastChange,
                            }
                            result = collection.insert_one(entry)
                            logger.info("Entry created with id: %s", result.inserted_id)
                        elif element.value == AccessStatus.Door.LockState.UNLOCKED:
                            logger.info("Door is unlocked")
                            entry = {
                                "doorLockStatus": door_lock_status,
                                "status": "door lock status",
                                "timestamp": element.lastChange,
                            }
                            result = collection.insert_one(entry)
                            logger.info("Entry created with id: %s", result.inserted_id)
                    elif ga.endswith("doors/frontLeft/openState"):
                        door_open_state = element.value.value if hasattr(element.value, "value") else element.value
                        if element.value == AccessStatus.Door.OpenState.OPEN:
                            logger.info("Driver Door is open")
                            entry = {
                                "doorOpenState": door_open_state,
                                "status": "door open state",
                                "timestamp": element.lastChange,
                            }
                            result = collection.insert_one(entry)
                            logger.info("Entry created with id: %s", result.inserted_id)
                        elif element.value == AccessStatus.Door.OpenState.CLOSED:
                            logger.info("Driver Door is closed")
                            entry = {
                                "doorOpenState": door_open_state,
                                "status": "door open state",
                                "timestamp": element.lastChange,
                            }
                            result = collection.insert_one(entry)
                            logger.info("Entry created with id: %s", result.inserted_id)
                    if ga.endswith("carCapturedTimestamp"):
                        doc = collection.find_one(
                            {"status": "door lock status"},
                            sort=[("timestamp", DESCENDING)]
                        )
                        if doc:
                            collection.update_one(
                                {"_id": doc["_id"]},
                                {"$set": {"carCapturedTimestamp": element.value, "status": "door lock status carCapturedTimestamp received"}}
                            )
                            logger.info("Entry updated with id: %s", doc['_id'])
                        doc = collection.find_one(
                            {"status": "door open state"},
                            sort=[("timestamp", DESCENDING)]
                        )
                        if doc:
                            collection.update_one(
                                {"_id": doc["_id"]},
                                {"$set": {"carCapturedTimestamp": element.value, "status": "door open state carCapturedTimestamp received"}}
                            )
                            logger.info("Entry updated with id: %s", doc['_id'])
                if ga.endswith("parking/parkingPosition/latitude"):
                    latitude_value = element.value
                    entry = {
                        "latitude": latitude_value,
                        "status": "latitude received",
                        "timestamp": element.lastChange,
                    }
                    result = collection.insert_one(entry)
                    logger.info("Entry created with id: %s", result.inserted_id)
                if ga.endswith("parking/parkingPosition/longitude"):
                    longitude_value = element.value
                    doc = collection.find_one({"status": "latitude received"}, sort=[("timestamp", DESCENDING)])
                    if doc:
                        collection.update_one(
                            {"_id": doc["_id"]},
                            {"$set": {"longitude": longitude_value, "status": "longitude received"}}
                        )
                        logger.info("Entry updated with id: %s", doc['_id'])
                if ga.endswith("parkingPosition/carCapturedTimestamp"):
                    doc = collection.find_one({"status": "longitude received"}, sort=[("timestamp", DESCENDING)])
                    if doc:
                        collection.update_one(
                            {"_id": doc["_id"]},
                            {"$set": {"carCapturedTimestamp": element.value, "status": "carCapturedTimestamp received"}}
                        )
                        logger.info("Entry updated with id: %s", doc['_id'])
            elif flags & addressable.AddressableLeaf.ObserverEvent.DISABLED:
                logger.info("Attribute is not available anymore: %s", element.getGlobalAddress())
    except Exception as ex:
        logger.exception("Exception in on_we_connect_event: %s", ex)

if __name__ == "__main__":
    main_loop()
