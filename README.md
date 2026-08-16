# The Blueprint Foundation: "Change is in the Air" Project - Data Pipeline
## Description
This repository contains code for all of the MQTT publishers and subscribers that are used to make up the data pipeline for the project.

## Components
### Subscriber: Ingester
This subscriber watches any topics matching the pattern `v1/+/readings`. The `+` acts as a _single_ field wildcard (as opposed to `*` that would match multiple "levels" of a topic string). This pattern is intended to normalize the topics that will be ingested from, while still allowing each publisher to have it's own topic.

Each message that is received then has it's data sent to a stored procedure in the Postgres database (see the `database` directory of the [Infrastructure repository](https://github.com/The-Blueprint-Foundation/TBF-Infrastructure) for the schema being used).

### Publisher: QuantAQ
It's designed to be a long-running script that executes an API request every hour with each resulting dataset being processed and published to the `v1/quantaq/readings` topic.

This publisher sources it's data from the [QuantAQ Cloud API](https://docs.quant-aq.com/). In order to use this API, an API Key for _The Blueprint Foundation_ will need to be acquired. From there, use the `/v1/devices` endpoint to get information on all devices associated with that key. Notice that **not all of the devices are attributed to _The Blueprint Foundation_**, so identifying the `organization_id` and `network_id` of the correct subset will be required. The script itself relies on the `/v1/data/most_recent` endpoint.

> **Note:** At the time of this writing, there are 6 devices associated with _The Blueprint Foundation_: 5 are MODULAIR models and 1 is MODULAIR-PM. This is noteworthy because, based on notes that can be seen in the `description` fields of the other (seemingly inactive) devices, the devices can be periodically swapped out with different models, which _might_ change what functionality is available and which fields are returned from the endpoint.

## BottleBot Support
All the necessary information pertaining to these devices can be found on their [main website](https://www.sensorbot.org/) and [GitHub repository](https://github.com/eykamp/birdhouse).

### Background  
One of the primary goals of this project was to create a new home for these devices to send their data to in order to feed a new frontend. The existing solution utilizes [ThingsBoard](https://thingsboard.io/), an open-source IoT management platform. It was originally recommended as a solution we continue with but, once some major hiccups happened with the project, and timelines shifted, it was decided to "strip" the ThingsBoard requirement down to it's barest essentials: an MQTT broker.

**Note:** Due to the roadblocks described below, development of an actual solution was not possible. A workaround will be developed for demonstration purposes but, in order to make it work correctly, it's GPS coordinates and identifier will be hardcoded since they can't be programmatically acquired through the process alone. For posterity, the plan will be documented in the ["Workaround" section](#workaround) below.

### Roadblocks
**GPS Coordinates:**  
It was _assumed_, based on the information gleaned from the QuantAQ API documentation, that the BottleBots would also be reporting their GPS location along with their sensor data; this assumption was incorrect. Instead, a bot's GPS location is determined upon the initial registration process when it's "host" supplies their home address.

> A solution to this issue is going to be implementation of a "registration handler" endpoint to the [project's API](https://github.com/The-Blueprint-Foundation/TBF-API) that would perform the same reverse-lookup process and recording to the database.

**Unique Identifiers:**  
The larger concern, it turns out, is that the devices all publish their messages into a single, generic, topic and they do not contain any unique device identifiers (ie. serial number) that would facilitate attributing that data back to it's device. The necessary identifying information _is_ being used as credential information, which is only useful with ThingsBoard being the MQTT broker. _However_, this is only an issue due to the use of the [Mosquitto](https://mosquitto.org/) broker for this portion of the project. Likely, the **simplest** solution to this deficiency is to replace _Mosquitto_ as the broker with one that, at the very least, supports the ability to intercept the messages, extract the credential information, and resubmit the messages to a different, device-specific, topic and/or inserts the new information into the original message.

Arguably, it would seem the best option would be to use the [ThingsBoard MQTT Broker](https://thingsboard.io/products/mqtt-broker/) which (unbeknownst to us at the time) is a stand-alone project that _might_ nullify this whole concern. Given that ThingsBoard is largely a UI-driven product, there might be a non-trivial learning curve to getting it working with the BottleBots, but the result would be better than a hacked-together solution that may not work all the time.

> It really seems like use of the TBMQ broker _should_ be all that is needed. More research would need to be performed to determine if it causes any problems with the QuantAQ solution in this repository. Had it been known as an option earlier, that's what would've been used and designed around.

### Workaround
The plan is to create a script that is a combination of subscriber and publisher. The _subscriber_ part will listen to the `v1/devices/me/telemetry` topic and, once it's received the two messages that consist of a full set of readings (see below for an example of the output) it will merge the message information into one and add in the hardcoded data that was mentioned above. Once the new message has been created, the _publisher_ portion will then publish that message to the topic `v1/bottlebot/readings`, so that it follows the pattern expected by the [Ingester](#subscriber-ingester).