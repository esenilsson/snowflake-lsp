import { TextEdit, Range } from 'vscode-languageserver/node';
import { TextDocument } from 'vscode-languageserver-textdocument';
import { spawn } from 'child_process';

export class FormattingProvider {
  /**
   * Format document using sqruff
   */
  async formatDocument(document: TextDocument): Promise<TextEdit[]> {
    const text = document.getText();

    try {
      const formatted = await this.runSqruff(text);

      // Return a single TextEdit that replaces entire document
      const fullRange = Range.create(
        document.positionAt(0),
        document.positionAt(text.length)
      );

      return [
        TextEdit.replace(fullRange, formatted)
      ];
    } catch (error) {
      console.error('Formatting error:', error);
      return [];
    }
  }

  /**
   * Run sqruff to format SQL
   */
  private async runSqruff(text: string): Promise<string> {
    return new Promise((resolve, reject) => {
      const sqruff = spawn('sqruff', ['fix', '-']);

      let stdout = '';
      let stderr = '';

      sqruff.stdout.on('data', (data) => {
        stdout += data.toString();
      });

      sqruff.stderr.on('data', (data) => {
        stderr += data.toString();
      });

      sqruff.on('close', (code) => {
        if (code === 0) {
          resolve(stdout);
        } else {
          reject(new Error(`sqruff exited with code ${code}: ${stderr}`));
        }
      });

      sqruff.on('error', (err) => {
        reject(new Error(`Failed to spawn sqruff: ${err.message}`));
      });

      // Write input to sqruff stdin
      sqruff.stdin.write(text);
      sqruff.stdin.end();
    });
  }
}
