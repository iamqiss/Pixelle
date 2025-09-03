<script lang="ts">
  let className = '';
  export { className as class };
  const lightLogo = "/messenger-apache-lightbg.svg";
  const darkLogo = "/messenger-apache-darkbg.svg";
</script>

<img src={lightLogo} class="{className} block dark:hidden" alt="Apache Messenger" />
<img src={darkLogo} class="{className} hidden dark:block" alt="Apache Messenger" />
