import pika
import json
import logging

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

def upload(f, fs, channel, access):
    try:
        fid = fs.put(f)
    except Exception as err:
        logger.error(f"Failed to store file: {str(err)}")
        return "internal server error: failed to store file", 500
    
    message = {
        "video_fid": str(fid),
        "mp3_fid": None,
        "username": access["username"],
    }

    try:
        channel.basic_publish(
            exchange="",
            routing_key="video",
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE, 
            )
        )
    except Exception as err:
        logger.error(f"Failed to publish message: {str(err)}")
        fs.delete(fid)
        return "internal server error: failed to queue job", 500

    return "success", 200