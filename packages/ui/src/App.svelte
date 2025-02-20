<script>
  import { onMount } from "svelte";

  let tables = [];
  console.log(import.meta.env.VITE_ICEBERG_URL);

  onMount(async () => {
    const response = await fetch(`${import.meta.env.VITE_ICEBERG_URL}/tables`);
    const data = await response.json();
    tables = data;
  });
</script>

<h1>Iceberg Tables</h1>
<ul>
  {#each tables as table}
    <li>{table.name}</li>
  {/each}
</ul>
