import os
from configparser import ConfigParser

from root import RESOURCE_DIR

ENV_CONST = ConfigParser()
ENV_CONST.read(RESOURCE_DIR + '/env_constants.ini')
ENV_CONFIG = ConfigParser()
RF_TRAN = ConfigParser()


def _set_env(env):
    global ENV_CONFIG, RF_TRAN

    assert env, 'Env not provided'
    
    envFile = RESOURCE_DIR + '/env_config_' + env + '.ini'
    if os.path.exists(envFile):
        ENV_CONFIG.read(envFile)

        rfParmFile = RESOURCE_DIR + '/rf_param_' + env + '.ini'
        if os.path.exists(envFile):
            RF_TRAN.read(rfParmFile)
        else:
            assert False, f"Rf param file not found {rfParmFile}"
    else:
        assert False, f"Env file not found {envFile}"
    # RF_TRAN = rf_param


# class Config:
#     ENV_CONFIG = ConfigParser()  # For specific env configs
#     # ENV_CONFIG.read(RESOURCE_DIR + '/env_config_test.ini')
#     ENV_CONST = ConfigParser()
#     ENV_CONST.read(RESOURCE_DIR + '/env_constants.ini')
#     RF_TRAN = rf_param  # For RF tran names
#
#     @classmethod
#     def _set_env(cls, env):
#         env = env.strip().lower()
#         cls.ENV_CONFIG.read(RESOURCE_DIR + '/env_config_' + env + '.ini')
#         if env is None:
#             assert False, "Please provide valid --env (eg: --env 'qa')"
#         cls.RF_TRAN = rf_param  # TODO Configure RF param file if different env
