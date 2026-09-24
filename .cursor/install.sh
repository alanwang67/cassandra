#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Cursor Cloud Agent bootstrap for Apache Cassandra.
#
# Installs the toolchain the Ant build expects and prepares the working tree:
#   * OpenJDK 11 (the default build JDK per build.xml java.default=11), plus
#     OpenJDK 17 and 21 (build.xml java.supported=11,17,21).
#   * Apache Ant and ant-optional (the latter provides the <junit> task used by
#     the test targets).
#   * Pins Java 11 as the system default and exports JAVA_HOME accordingly.
#   * Checks out the Accord submodule, which the build requires.
#
# The script is idempotent: apt-get install, update-alternatives --set and the
# submodule update are all safe to re-run.

set -o errexit
set -o pipefail
set -o nounset

export DEBIAN_FRONTEND=noninteractive

# .cursor/install.sh lives one level below the project root.
home="$(cd "$(dirname "$0")/.." > /dev/null; pwd)"
cd "$home"

sudo apt-get update
sudo apt-get install -y --no-install-recommends \
  openjdk-11-jdk-headless \
  openjdk-17-jdk-headless \
  openjdk-21-jdk-headless \
  ant \
  ant-optional

java11_home="/usr/lib/jvm/java-11-openjdk-$(dpkg --print-architecture)"
sudo update-alternatives --set java "$java11_home/bin/java"
sudo update-alternatives --set javac "$java11_home/bin/javac"

if ! grep -q '^JAVA_HOME=' /etc/environment; then
  echo "JAVA_HOME=$java11_home" | sudo tee -a /etc/environment > /dev/null
fi

# Accord is a git submodule that the build depends on.
git submodule update --init --recursive

echo "Cassandra Cloud Agent environment ready:"
java -version
ant -version
