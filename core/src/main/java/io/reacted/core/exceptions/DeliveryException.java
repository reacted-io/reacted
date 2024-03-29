/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.exceptions;

public class DeliveryException extends RuntimeException {
  public DeliveryException() { }
  public DeliveryException(Exception realCause) { super(realCause); }
}
