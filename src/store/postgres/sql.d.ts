declare module "*.sql" {
  /** The file contents, inlined at build time. */
  const text: string;
  export default text;
}
