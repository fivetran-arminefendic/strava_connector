![strava logo](https://cdn.iconscout.com/icon/free/png-256/strava-2752062-2284879.png)
# Strava Connector
This code is made to work in conjuction with a [Fivetran custom connector](https://fivetran.com/docs/functions/google-cloud-functions#googlecloudfunctions) using a Google Cloud Function. 

- This connector requests data from the Strava API
- A Strava app was created and users are sent an oauth link to give access to the Strava App.
- Once permissions are granted a google sheet is used to store the users refresh token. 
- This refresh token is used to grab user data from Strava.