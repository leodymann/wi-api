from app.integrations.blibsend import send_whatsapp_text

if __name__ == "__main__":
    send_whatsapp_text(
        to="55839871577461",
        body="Teste whatsapp.",
    )
    print("OK")
