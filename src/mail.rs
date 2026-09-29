use serde_json::json;

pub struct Mailer {
    api_key: String,
    from: String,
    http: reqwest::Client,
}

impl Mailer {
    pub fn new(api_key: String, from: String) -> Self {
        Self {
            api_key,
            from,
            http: reqwest::Client::new(),
        }
    }

    pub async fn send_html(&self, to: &str, subject: &str, html: &str) -> Result<(), ()> {
        let response = self
            .http
            .post("https://api.resend.com/emails")
            .bearer_auth(&self.api_key)
            .json(&json!({
                "from": self.from,
                "to": [to],
                "subject": subject,
                "html": html,
            }))
            .send()
            .await
            .map_err(|err| tracing::warn!("resend: {err}"))?;
        if response.status().is_success() {
            Ok(())
        } else {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            tracing::warn!("resend error {status}: {body}");
            Err(())
        }
    }
}
