import params
import re
import requests
from requests.auth import HTTPDigestAuth
from pymongo import MongoClient

# These are supported options on Atlas' API as of March, 2022
SUPPORTED_ACTIONS = [
    "FIND",
    "INSERT",
    "REMOVE",
    "UPDATE",
    "BYPASS_DOCUMENT_VALIDATION",
    "USE_UUID",
    "CREATE_COLLECTION",
    "CREATE_INDEX",
    "DROP_COLLECTION",
    "ENABLE_PROFILER",
    "CHANGE_STREAM",
    "COLL_MOD",
    "COMPACT",
    "CONVERT_TO_CAPPED",
    "DROP_DATABASE",
    "DROP_INDEX",
    "RE_INDEX",
    "RENAME_COLLECTION_SAME_DB",
    "LIST_SESSIONS",
    "KILL_ANY_SESSION",
    "COLL_STATS",
    "CONN_POOL_STATS",
    "DB_HASH",
    "DB_STATS",
    "LIST_DATABASES",
    "LIST_COLLECTIONS",
    "LIST_INDEXES",
    "SERVER_STATUS",
    "VALIDATE",
    "TOP",
    "SQL_GET_SCHEMA",
    "SQL_SET_SCHEMA",
    "VIEW_ALL_HISTORY",
    "OUT_TO_S3",
    "STORAGE_GET_CONFIG",
    "STORAGE_SET_CONFIG",
]


def post_atlas_api(url, data, key):
    headers = {"content-type": "application/json"}
    resp = requests.post(
        url=url,
        auth=HTTPDigestAuth(params.target_api_user, params.target_api_key),
        json=data,
        headers=headers,
    )

    if resp.status_code == 201:
        print("User " + data[key] + " created.")
    elif resp.status_code == 202:
        print("Role " + data[key] + " created.")
    elif resp.status_code == 409:
        print(data[key] + " alredy exists.")
    else:
        print(data[key] + " failed to be created.")
        print("Error - status code: " + str(resp.status_code))
        print(resp.text)


def format_actions(unformatted_actions):
    """
    Format actions object for Atlas API
    """
    actions = []
    for unformatted_action in unformatted_actions.keys():
        action = {
            "action": unformatted_action,
            "resources": unformatted_actions[unformatted_action],
        }
        actions.append(action)
    return actions


def convert_privilege_to_unformatted_actions(privilege, actions):
    """
    Converts pivilege objects from source cluster to map where keys are actions and values are
    """
    action_names = privilege["actions"]
    for action in action_names:
        action = re.sub(r"(?<!^)(?=[A-Z])", "_", action).upper()
        if not action in SUPPORTED_ACTIONS:
            print(f"{action} is not supported on atlas, skipping")
            continue
        if not action in actions.keys():
            actions[action] = []
        print("*** " + str(privilege["resource"]))
        if privilege["resource"]["collection"].startswith("system"):
            print("skipping " + privilege["resource"]["collection"])
        else:
            if privilege["resource"]["collection"] is None:
                print("no collection: " + str(privilege["resource"]))
            else:    
                actions[action].append(privilege["resource"])
    return actions


def convert_privileges_to_actions(privileges):
    """
    Performs transformation from Mongosh's privilege object containing a list of action
    strings to Atlas' API required list of action objects that have a list of resources
    """
    unformatted_actions = {}
    for privilege in privileges:
        unformatted_actions = convert_privilege_to_unformatted_actions(
            privilege, unformatted_actions
        )
    return format_actions(unformatted_actions)


def migrate_roles():
    """
    Pull roles from source cluster and POST them to Atlas API using credentials in params.py
    """
    roles_info = db.command({"rolesInfo": 1, "showPrivileges": True})
    roles = roles_info["roles"]
    print(str(len(roles)) + " potential roles to be migrated\n")

    url = (
        "https://cloud.mongodb.com/api/atlas/v1.0/groups/"
        + params.target_project_id
        + "/customDBRoles/roles"
    )

    for role in roles:
        if role["isBuiltin"]:
            continue
        actions = convert_privileges_to_actions(role["privileges"])
        #print(actions)
        formatted_role = {
            "roleName": role["role"],
            "actions": actions,
            "inheritedRoles": role["inheritedRoles"],
        }

        print(">>> Migrating role:")
        print(str(formatted_role["roleName"]) + "\n")

        print("\nUser data sent to Atlas API:")
        print(formatted_role)
        print()
        post_atlas_api(url, formatted_role, "roleName")

        print(">>>\n")


def format_user_roles(roles):
    """
    Format roles object for Atlas API
    """
    result = []
    for role in roles:
        formatted_role = {"databaseName": role["db"], "roleName": role["role"]}
        result.append(formatted_role)
    print("Formatted roles:")
    print(result)
    return result


def migrate_users():
    """
    Pull users from source cluster and POST them to Atlas API using credentials in params.py
    """
    user_info = db.command("usersInfo")
    users = user_info["users"]
    print(str(len(users)) + " potential users to be migrated\n")

    # Format the roles as required by the Atlas API

    headers = {"content-type": "application/json"}

    url = (
        "https://cloud.mongodb.com/api/atlas/v1.0/groups/"
        + params.target_project_id
        + "/databaseUsers"
    )

    ## Create users
    for user in users:

        # Don't try to create agent users in Atlas
        if user["user"][:3] == "mms":
            print("<<<Skipping user " + user["user"] + ">>>\n")
            continue

        print(">>> Migrating user:")
        print(str(user) + "\n")

        formatted_user = {
            "databaseName": "admin",
            "roles": format_user_roles(user["roles"]),
            "username": user["user"],
            "password": "changeme123",
        }

        print("\nUser data sent to Atlas API:")
        print(formatted_user)
        print()

        post_atlas_api(url, formatted_user, "username")
        print(">>>\n")


print("\nMigrating MongoDB Users\n")
# Establish connection to the source cluster
client = MongoClient(params.source_conn_string)
db = client[params.source_database]
migrate_roles()
migrate_users()
