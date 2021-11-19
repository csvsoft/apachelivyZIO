package com

import sttp.client.asynchttpclient.zio.SttpClient
import zio.clock.Clock
import zio.console.Console

package object test{
  type LivyR = SttpClient with Clock with Console
}
