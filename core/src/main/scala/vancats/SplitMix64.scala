/*
 * Copyright 2021 Arman Bilge
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package vancats

import cats.data.State

// Via https://prng.di.unimi.it/splitmix64.c
// SplitMix64 never repeats itself, which is bad for rngs but great for us!

private[vancats] val SplitMix64 = State[Long, Long] { _x =>
  var x = _x + 0x9e3779b97f4a7c15L
  var z = x
  z = (z ^ z >>> 30) * 0xbf58476d1ce4e5b9L
  z = (z ^ z >>> 27) * 0x94d049bb133111ebL
  z = z ^ z >>> 31;
  (x, z)
}
