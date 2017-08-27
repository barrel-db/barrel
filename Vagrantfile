# -*- mode: ruby -*-
# vi: set ft=ruby :

Vagrant.configure("2") do |config|
  
  config.vm.provider "vmware_fusion" do |v|
    v.vmx["memsize"] = "4096"
    v.vmx["numvcpus"] = "2"
  end

  config.vm.provider "virtualbox" do |v|
    v.memory = 4096
    v.cpus = 2
  end

  config.vm.define "ubuntu" do |box|
    box.vm.box = "bento/ubuntu-16.04"

    box.vm.provision "shell", inline: <<-SCRIPT
      curl -O https://raw.githubusercontent.com/kerl/kerl/master/kerl
      chmod a+x kerl
      touch ~/.kerlrc
      cat 'KERL_CONFIGURE_OPTIONS="-enable-shared-zlib --disable-dynamic-ssl-lib --disable-hipe --enable-smp-support --enable-threads --enable-kernel-poll --enable-dirty-schedulers"' > ~/.kerlrc

      export DEBIAN_FRONTEND=noninteractive
      apt-get update
      apt-get install -y build-essential autoconf libncurses5-dev openssl libssl-dev fop xsltproc unixodbc-dev git

      ./kerl build git git://github.com/erlang/otp.git OTP-20.0.2 OTP-20.0.2
      ./kerl install OTP-20.0.2 /srv/otp/20.0

      chmod -R 775 /srv/otp/20.0

      echo ". /srv/otp/20.0/activate" >> /home/vagrant/.bashrc
    SCRIPT
  end

  config.vm.define "FreeBSD11" do |box|
    box.vm.box = "freebsd/FreeBSD-11.0-STABLE"
    box.ssh.shell = "sh"
    box.vm.synced_folder ".", "/vagrant", :nfs => true, id: "vagrant-root"

    box.vm.provision "shell", inline: <<-SCRIPT
      curl -O https://raw.githubusercontent.com/kerl/kerl/master/kerl
      chmod +x kerl
      touch ~/.kerlrc
      cat 'KERL_CONFIGURE_OPTIONS="-enable-shared-zlib --disable-dynamic-ssl-lib --disable-hipe --enable-smp-support --enable-threads --enable-kernel-poll --enable-dirty-schedulers"' > ~/.kerlrc

      pkg update
      pkg install autoconf gcc bash gmake flex git curl

      ./kerl build git git://github.com/erlang/otp.git OTP-20.0.2 OTP-20.0.2
      ./kerl install OTP-20.0.2 /usr/local/otp/20.0

      chmod -R 775  /usr/local/otp/20.0

      echo "source /srv/otp/20.0/activate" >> /usr/home/vagrant/.tchrc

    SCRIPT
  end
end
