import os.path

_LOCAL_PATH = os.path.expanduser('~/.s2s/')
_TSS_PATH = '/var/lib/tss/keys/s2s/'

def resolve_from_service_identifier(ident):
  if ident.zone == 'local':
    root_path = _LOCAL_PATH
  else:
    root_path = _TSS_PATH

  creds_path = os.path.join(root_path, ident.zone, ident.env, ident.service, ident.role)
  return (
    os.path.join(creds_path, 'client.chain'),
    os.path.join(creds_path, 'client.key'),
    os.path.join(creds_path, 'client.crt'))

