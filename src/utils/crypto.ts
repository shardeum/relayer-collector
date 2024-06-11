import * as core from '@shardus/crypto-utils'
import { SignedObject } from '@shardus/crypto-utils'
import { Utils as StringUtils } from '@shardus/types'

import { config as COLLECTOR_CONFIG } from '../config'

// Crypto initialization fns

export function setCryptoHashKey(hashkey: string): void {
  core.init(hashkey)
  core.setCustomStringifier(StringUtils.safeStringify, 'shardus_safeStringify')
}

export const hashObj = core.hashObj


// Asymmetric Encyption Sign/Verify API
export type SignedMessage = SignedObject

export function sign<T>(obj: T): T & SignedObject {
  const objCopy = StringUtils.safeJsonParse(StringUtils.safeStringify(obj))
  core.signObj(objCopy, COLLECTOR_CONFIG.collectorInfo.secretKey, COLLECTOR_CONFIG.collectorInfo.publicKey)
  return objCopy
}

export function verify(obj: SignedObject): boolean {
  return core.verifyObj(obj)
}
