import voluptuous as vol
from homeassistant import config_entries
from homeassistant.const import CONF_USERNAME, CONF_PASSWORD
from .const import CLIENT_ID, CLIENT_SECRET

DATA_SCHEMA = vol.Schema({
    vol.Required(CONF_USERNAME): str,
    vol.Required(CONF_PASSWORD): str,
})

class IntuisConfigFlow(config_entries.ConfigFlow, domain="intuis_connect"):
    VERSION = 1

    async def async_step_user(self, user_input=None):
        errors = {}
        if user_input is not None:
            # login via API avec CLIENT_ID/CLIENT_SECRET embarqués
            try:
                ok = await self._try_login(
                    user_input[CONF_USERNAME],
                    user_input[CONF_PASSWORD],
                )
            except Exception:
                ok = False
            if ok:
                return self.async_create_entry(
                    title="Intuis Connect",
                    data={
                        CONF_USERNAME: user_input[CONF_USERNAME],
                        CONF_PASSWORD: user_input[CONF_PASSWORD],
                        # Rien d’autre à stocker: tokens seront gérés/rafraîchis au runtime
                    },
                )
            errors["base"] = "auth"
        return self.async_show_form(step_id="user", data_schema=DATA_SCHEMA, errors=errors)

    async def _try_login(self, username: str, password: str) -> bool:
        # Appelle ton client API interne ici, en passant CLIENT_ID/CLIENT_SECRET
        # return await api.login(username, password, CLIENT_ID, CLIENT_SECRET)
        return True
