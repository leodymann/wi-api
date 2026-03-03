from app.infra.storage_s3 import (
    upload_image_bytes,
    delete_object_best_effort,
    presign_get_url,
)

@router.get("/_debug/s3")
def debug_s3():
    try:
        # 1️⃣ Faz upload de um arquivo fake
        key = upload_image_bytes(
            data=b"test-image",
            content_type="image/png",
            key_prefix="debug"
        )

        # 2️⃣ Gera presigned URL
        presigned_url = presign_get_url(key, expires_seconds=3600)

        return {
            "ok": True,
            "key": key,
            "presigned_url": presigned_url,
            "expires_in_seconds": 3600
        }

    except Exception as e:
        print("DEBUG S3 ERROR:", repr(e))
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
