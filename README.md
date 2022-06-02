# TH2 Act Template (Python)

## Overview
This repository is a template of *th2-act* application on Python.
It contains the implementation of the *handlers* and the entry point. 
The main logic and everything you need to create a *handler* is contained in the **th2_act** 
library from **th2-pypi** Nexus repository.

This template implements gRPC API described in the [th2-grpc-act-template](https://github.com/th2-net/th2-grpc-act-template/tree/th2-2549/src/main/proto/th2_grpc_act_template). 
Several services can be described and used at once (`Act` and `ActTyped` in this
template). Each service needs an ActHandler (look at *handlers* folder).

## Getting started

Steps to implement Act:

0. Create a custom gRPC API (in `th2-act-template` [th2-grpc-act-template](https://github.com/th2-net/th2-grpc-act-template/tree/th2-2549/src/main/proto/th2_grpc_act_template) id used).

1. Make fork of `th2-act-template` repository and clone it on the local machine.

2. Change gRPC API name and version in *requirements.txt*. Install all required packages:
```
pip install -r requirements.txt
```

3. Now let's move on to creating custom handlers. Instructions for creating **non-typed** and 
**typed** handlers are listed below. 


## Creating act-handlers

Instructions below are common for both **non-typed** and **typed** handlers.

1. In `handlers/` folder create new python file, for example `my_handler.py`.

2. Create class named `ActHandler`(the Act will not be able to upload the handler if the class has 
another name) inherited from `ActServicer` class from your gRPC API. In the code below
`ActHandler` class already contains `__init__()` method, just copy it.
```python
from th2_act import ActConnector

from th2_grpc_act_template import act_template_pb2_grpc


class ActHandler(act_template_pb2_grpc.ActServicer):
   
    def __init__(self, act_conn: ActConnector):
        self.act_conn = act_conn
```

3. Add methods described in your gRPC API in `ActHandler`:
```python
# import ...

class ActHandler(act_template_pb2_grpc.ActServicer):
   
    def __init__(self, act_conn: ActConnector):
        self.act_conn = act_conn

    def placeOrderFIX(self, request, context):
        pass

    def sendMessage(self, request, context):
       pass

    def placeQuoteRequestFIX(self, request, context):
       pass

    def placeQuoteFIX(self, request, context):
       pass

    def placeOrderMassCancelRequestFIX(self, request, context):
       pass

    def placeQuoteCancelFIX(self, request, context):
       pass

    def placeQuoteResponseFIX(self, request, context):
       pass
```

4. Now it is necessary to implement each method. Add new import:
```python
from th2_act import ActParameters
```
Then initialize `ActParameters` class instance in each `ActHandler` method, for example:
```python
def placeOrderFIX(self, request, context):
    act_parameters = ActParameters(act_name='Place order FIX',
                                   request_event_id=request.parent_event_id,
                                   request_description=request.description,
                                   context=context)
```
> **Note:** your `request` class from gRPC API should have `parent_event_id` and `description` fields 
The fields could be named differently, but should be type of `EventID` and `string` respectively.

Next step is initializing `RequestProcessor` context manager, which will be used for sending and receiving
messages:
```python
def placeOrderFIX(self, request, context):
    act_parameters = ActParameters(act_name='Place order FIX',
                                   request_event_id=request.parent_event_id,
                                   request_description=request.description,
                                   context=context)
    
    with RequestProcessor(self.act_conn, act_parameters) as rp:
        # ...
```

5. Now, we will consider separately the work of `RequestProcessor` in **non-typed** and **typed** handlers.
   
   5.1. `RequestProcessor` for **non-typed** handler.
   ```python
   from th2_grpc_common.common_pb2 import RequestStatus

   
   def placeOrderFIX(self, request, context):
        act_parameters = ...
   
        with RequestProcessor(self.act_conn, act_parameters) as rp:
            # first step: send message
            request_msg = request.message  # get th2-message from request 
                                           # (you send send any message, not only message from request)
            rp.send(request_msg)  # send th2-message
   
            # second step: describe filter (or filters) for response message (or messages)
            # filter should be lambda function
            condition = lambda response_msg: response_msg['ClOrdID'] == request_msg['ClOrdID'] and \
                                             response_msg.metadata.message_type == 'ExecutionReport'
   
            # third step: wait first matching response
            # in condition_set parameter specify the dict with your condition as key 
            # and status (success or error) as value
            response = rp.wait_first_matching(conditions_set={condition: RequestStatus.SUCCESS})
   
        # finally, wrap response in your gRPC class
        return PlaceMessageResponse(response_message=response.message,
                                    status=response.status,
                                    checkpoint_id=response.checkpoint)
   ```
   
   5.2. `RequestProcessor` for **typed** handler.
   ```python
   from th2_grpc_common.common_pb2 import RequestStatus

   from th2_act_template.custom import support_functions as sf

   
   def placeOrderFIX(self, request, context):
        act_parameters = ...
   
        with RequestProcessor(self.act_conn, act_parameters) as rp:
            # first step: send message
            request_msg = sf.get_message_from_typed_request(request)  # get th2-message from typed request using custom function
                                                                      # (you can send any message, not only message from request)
            rp.send(request_msg)  # send th2-message
   
            # second step: describe filter (or filters) for response message (or messages)
            # filter should be lambda function
            condition = lambda response_msg: response_msg['ClOrdID'] == request_msg['ClOrdID'] and \
                                             response_msg.metadata.message_type == 'ExecutionReport'
   
            # third step: wait all matching responses
            # in condition_set parameter specify the dict with your condition as key and status (success or error) as value
            # in check_frequency you can set time (in seconds) which Act will wait for responses (defaults to 2 s)
            response = rp.wait_all_matching(conditions_set={condition: RequestStatus.SUCCESS}
                                            check_frequency=10)

        # forth step: convert non-typed responses into typed
        typed_responses = [sf.create_typed_response_from_notification(response) for response in responses]

        # finally, wrap response in your gRPC class
        return PlaceMessageResponseTypedList(place_message_response_typed=typed_responses)
   ```

You can find full code of **non-typed** handler [here](https://gitlab.exactpro.com/vivarium/th2/th2-core-proprietary/th2-act-template-py/-/blob/th2-2871/th2_act_template/handlers/template_handler.py) 
and for **typed** handler [here](https://gitlab.exactpro.com/vivarium/th2/th2-core-proprietary/th2-act-template-py/-/blob/th2-2871/th2_act_template/handlers/template_handler_typed.py).
